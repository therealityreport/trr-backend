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
import re
import unicodedata
from collections.abc import AsyncGenerator, Callable
from datetime import UTC, datetime
from typing import Any, Literal
from urllib.parse import unquote, urlparse
from uuid import UUID

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from api.auth import AdminUser, FacebankSeedAdminUser
from api.deps import SupabaseAdminClient
from trr_backend.repositories.media_links import update_media_link_facebank_seed

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/person", tags=["admin-person"])

# Valid sources for person images
SourceType = Literal["imdb", "tmdb", "fandom", "fandom-gallery"]
ALL_SOURCES: list[SourceType] = ["imdb", "tmdb", "fandom", "fandom-gallery"]
TEXT_OVERLAY_FAILURE_REASONS = (
    "download_failed",
    "gemini_request_failed",
    "gemini_no_text",
    "gemini_json_parse_failed",
    "db_update_failed",
)


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
    show_id: UUID | None = Field(
        default=None,
        description="Optional show context to tag all fetched photos with show_id/show_name for filtering.",
    )
    show_name: str | None = Field(
        default=None,
        description="Optional show name to tag all fetched photos with show_id/show_name for filtering.",
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
    auto_counts_attempted: int = 0
    auto_counts_succeeded: int = 0
    auto_counts_failed: int = 0
    text_overlay_attempted: int = 0
    text_overlay_succeeded: int = 0
    text_overlay_unknown: int = 0
    text_overlay_failed: int = 0
    text_overlay_failure_reasons: dict[str, int] = Field(default_factory=dict)
    episode_metadata_tagged: int = 0
    show_context_tagged: int = 0
    metadata_enrichment_failed: int = 0
    centering_attempted: int = 0
    centering_succeeded: int = 0
    centering_failed: int = 0
    centering_skipped_manual: int = 0
    errors: list[str] = Field(default_factory=list)


class FacebankSeedRequest(BaseModel):
    facebank_seed: bool = Field(..., description="Whether this image should seed facebank.")


class FacebankSeedResponse(BaseModel):
    link_id: str
    person_id: str
    facebank_seed: bool


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


def _get_show_name(db: SupabaseAdminClient, show_id: UUID | None) -> str | None:
    if show_id is None:
        return None
    response = (
        db.schema("core")
        .table("shows")
        .select("name")
        .eq("id", str(show_id))
        .limit(1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        return None
    if not response.data:
        return None
    name = response.data[0].get("name")
    return str(name).strip() if isinstance(name, str) and name.strip() else None


def _is_real_housewives_show(show_name: str | None) -> bool:
    if not isinstance(show_name, str):
        return False
    normalized = show_name.strip().lower()
    return bool(normalized) and "real housewives" in normalized


def _apply_show_source_policy(
    db: SupabaseAdminClient,
    show_id: UUID | None,
    sources: list[SourceType],
) -> tuple[list[SourceType], bool]:
    show_name = _get_show_name(db, show_id)
    if show_name is None or _is_real_housewives_show(show_name):
        return sources, False

    blocked = {"fandom", "fandom-gallery"}
    filtered_sources = [source for source in sources if source not in blocked]
    fandom_skipped = len(filtered_sources) != len(sources)
    return filtered_sources, fandom_skipped


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


def _is_http_url(value: str | None) -> bool:
    if not isinstance(value, str):
        return False
    trimmed = value.strip().lower()
    return trimmed.startswith("http://") or trimmed.startswith("https://")


def _is_wikia_static_url(value: str | None) -> bool:
    if not _is_http_url(value):
        return False
    return "static.wikia.nocookie.net" in value.lower()


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


def _pick_autocount_urls(row: dict[str, Any]) -> list[str]:
    from trr_backend.media.s3_mirror import normalize_fandom_file_url

    source = str(row.get("source") or "").lower()
    image_url = row.get("image_url") or row.get("url")
    raw_url = row.get("url")
    thumb_url = row.get("thumb_url")
    hosted_url = row.get("hosted_url")
    referer = row.get("source_page_url") if isinstance(row.get("source_page_url"), str) else None

    if source == "tmdb":
        return _iter_unique_urls([image_url, raw_url, hosted_url, thumb_url])

    if source in ("fandom", "fandom-gallery"):
        normalized = [
            normalize_fandom_file_url(str(value), referer=referer) if isinstance(value, str) else None
            for value in (image_url, raw_url, thumb_url)
        ]
        return _iter_unique_urls([hosted_url, *normalized, image_url, raw_url, thumb_url])

    return _iter_unique_urls([hosted_url, image_url, raw_url, thumb_url])


def _pick_autocount_url(row: dict[str, Any]) -> str | None:
    urls = _pick_autocount_urls(row)
    return urls[0] if urls else None


def _is_manual_thumbnail_crop(value: Any) -> bool:
    return isinstance(value, dict) and str(value.get("mode") or "").lower() == "manual"


def _should_recenter_auto_crop(existing_crop: Any, *, force: bool = False) -> bool:
    if _is_manual_thumbnail_crop(existing_crop):
        return False
    if force:
        return True
    if not isinstance(existing_crop, dict):
        return True
    mode = str(existing_crop.get("mode") or "").lower()
    if mode != "auto":
        return True
    strategy = str(existing_crop.get("strategy") or "").lower()
    if strategy != "face_torso_v2":
        return True
    for key in ("x", "y", "zoom"):
        value = existing_crop.get(key)
        if not isinstance(value, (int, float)):
            return True
    return False


def _build_media_link_autocount_urls(row: dict[str, Any]) -> list[str]:
    from trr_backend.media.s3_mirror import normalize_fandom_file_url

    source = str(row.get("source") or "").lower()
    hosted_url = row.get("hosted_url")
    source_url = row.get("source_url")
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    source_page_url = (
        metadata.get("page_url")
        if isinstance(metadata.get("page_url"), str)
        else metadata.get("source_page_url")
        if isinstance(metadata.get("source_page_url"), str)
        else None
    )

    if source in {"fandom", "fandom-gallery"} and isinstance(source_url, str):
        normalized = normalize_fandom_file_url(source_url, referer=source_page_url)
        return _iter_unique_urls([hosted_url, normalized, source_url])
    return _iter_unique_urls([hosted_url, source_url])


def _fetch_person_media_link_rows(
    db: SupabaseAdminClient,
    person_id: str,
) -> list[dict[str, Any]]:
    links_resp = (
        db.schema("core")
        .table("media_links")
        .select("id, media_asset_id, context")
        .eq("entity_type", "person")
        .eq("entity_id", person_id)
        .eq("kind", "gallery")
        .execute()
    )
    if hasattr(links_resp, "error") and links_resp.error:
        logger.warning("Media links query failed for %s: %s", person_id, links_resp.error)
        return []

    links = links_resp.data or []
    if not links:
        return []

    asset_ids = [str(link.get("media_asset_id")) for link in links if link.get("media_asset_id")]
    if not asset_ids:
        return []

    assets_resp = (
        db.schema("core")
        .table("media_assets")
        .select("id, source, source_url, hosted_url, metadata")
        .in_("id", asset_ids)
        .execute()
    )
    if hasattr(assets_resp, "error") and assets_resp.error:
        logger.warning("Media assets query failed for %s: %s", person_id, assets_resp.error)
        return []
    assets_by_id = {str(row.get("id")): row for row in (assets_resp.data or []) if row.get("id")}

    rows: list[dict[str, Any]] = []
    for link in links:
        asset_id = str(link.get("media_asset_id") or "")
        if not asset_id:
            continue
        asset = assets_by_id.get(asset_id)
        if not asset:
            continue
        rows.append(
            {
                "id": str(link.get("id")),
                "media_asset_id": asset_id,
                "context": link.get("context") if isinstance(link.get("context"), dict) else {},
                "source": asset.get("source"),
                "source_url": asset.get("source_url"),
                "hosted_url": asset.get("hosted_url"),
                "metadata": asset.get("metadata") if isinstance(asset.get("metadata"), dict) else {},
            }
        )
    return rows


def _apply_auto_crop_payload(
    result: Any,
    *,
    fallback_strategy: str = "face_centroid_v1",
) -> dict[str, Any] | None:
    from trr_backend.clients.screenalytics import auto_thumbnail_crop, face_centroid

    generated = auto_thumbnail_crop(result)
    if generated is not None:
        return {
            **generated,
            "generated_at": datetime.now(UTC).isoformat(),
        }
    centroid = face_centroid(result)
    if centroid is None:
        return None
    cx, cy = centroid
    return {
        "x": cx,
        "y": cy,
        "zoom": 1,
        "mode": "auto",
        "strategy": fallback_strategy,
        "generated_at": datetime.now(UTC).isoformat(),
    }


def _recenter_person_gallery_images(
    db: SupabaseAdminClient,
    person_id: str,
    sources: list[SourceType],
    *,
    photo_ids: list[str] | None = None,
    progress_cb: Callable[[int, int], None] | None = None,
    force: bool = False,
) -> tuple[int, int, int, int]:
    attempted = 0
    succeeded = 0
    failed = 0
    skipped_manual = 0

    candidate_sources = [s for s in sources if s in ALL_SOURCES]
    if not candidate_sources:
        return attempted, succeeded, failed, skipped_manual

    try:
        from trr_backend.clients.screenalytics import (
            ScreenalyticsClientError,
            count_people,
            is_screenalytics_configured,
        )

        if not is_screenalytics_configured():
            return attempted, succeeded, failed, skipped_manual

        cast_query = (
            db.schema("core")
            .table("cast_photos")
            .select("id, hosted_url, url, image_url, thumb_url, source_page_url, source, metadata")
            .eq("person_id", person_id)
            .in_("source", candidate_sources)
        )
        if photo_ids:
            cast_query = cast_query.in_("id", photo_ids)
        cast_rows = cast_query.execute().data or []

        media_rows = _fetch_person_media_link_rows(db, person_id)

        to_process: list[dict[str, Any]] = []
        for row in cast_rows:
            metadata = dict(row.get("metadata") or {})
            existing_crop = metadata.get("thumbnail_crop")
            if _is_manual_thumbnail_crop(existing_crop):
                skipped_manual += 1
                continue
            if not _should_recenter_auto_crop(existing_crop, force=force):
                continue
            urls = _pick_autocount_urls(row)
            if not urls:
                continue
            to_process.append(
                {
                    "origin": "cast_photos",
                    "id": str(row.get("id")),
                    "urls": urls,
                    "metadata": metadata,
                }
            )

        for row in media_rows:
            context = dict(row.get("context") or {})
            existing_crop = context.get("thumbnail_crop")
            if _is_manual_thumbnail_crop(existing_crop):
                skipped_manual += 1
                continue
            if not _should_recenter_auto_crop(existing_crop, force=force):
                continue
            urls = _build_media_link_autocount_urls(row)
            if not urls:
                continue
            to_process.append(
                {
                    "origin": "media_links",
                    "id": str(row.get("id")),
                    "urls": urls,
                    "context": context,
                }
            )

        total = len(to_process)
        for idx, entry in enumerate(to_process, start=1):
            attempted += 1
            result = None
            last_error: ScreenalyticsClientError | None = None
            for image_url in entry["urls"]:
                try:
                    result = count_people(image_url)
                    break
                except ScreenalyticsClientError as exc:
                    last_error = exc
            try:
                if result is None:
                    raise last_error or ScreenalyticsClientError("Unable to center/crop image")
                crop_payload = _apply_auto_crop_payload(result)
                if crop_payload is None:
                    raise ScreenalyticsClientError("No detections available for centering/cropping")
                if entry["origin"] == "cast_photos":
                    metadata = dict(entry["metadata"] or {})
                    metadata["thumbnail_crop"] = crop_payload
                    db.schema("core").table("cast_photos").update({"metadata": metadata}).eq(
                        "id", entry["id"]
                    ).execute()
                else:
                    context = dict(entry["context"] or {})
                    context["thumbnail_crop"] = crop_payload
                    db.schema("core").table("media_links").update(
                        {
                            "context": context,
                            "updated_at": datetime.now(UTC).isoformat(),
                        }
                    ).eq("id", entry["id"]).execute()
                succeeded += 1
                logger.info(
                    "Centering crop saved origin=%s id=%s strategy=%s x=%.1f y=%.1f zoom=%.2f",
                    entry["origin"],
                    entry["id"],
                    str(crop_payload.get("strategy") or "unknown"),
                    float(crop_payload.get("x", 50)),
                    float(crop_payload.get("y", 32)),
                    float(crop_payload.get("zoom", 1)),
                )
            except Exception as exc:  # noqa: BLE001
                failed += 1
                logger.warning(
                    "Centering crop failed origin=%s id=%s error=%s",
                    entry["origin"],
                    entry["id"],
                    exc,
                )
            if progress_cb:
                progress_cb(idx, total)
    except Exception as exc:
        logger.exception("Centering/cropping setup failed for %s: %s", person_id, exc)

    return attempted, succeeded, failed, skipped_manual


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
    rows = fetch_cast_photos_missing_hosted(db, person_ids=[person_id], cdn_base_url=cdn_base_url, include_hosted=force)
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


def _auto_count_cast_photos(
    db: SupabaseAdminClient,
    person_id: str,
    sources: list[SourceType],
    *,
    photo_ids: list[str] | None = None,
    force_recount: bool = False,
    progress_cb: Callable[[int, int], None] | None = None,
) -> tuple[int, int, int]:
    """Auto-count people for selected cast photos. Returns (attempted, succeeded, failed)."""
    auto_counts_attempted = 0
    auto_counts_succeeded = 0
    auto_counts_failed = 0

    candidate_sources = [s for s in sources if s in ALL_SOURCES]
    if not candidate_sources:
        return auto_counts_attempted, auto_counts_succeeded, auto_counts_failed
    if photo_ids is not None and not photo_ids:
        # No new photos, but still allow backfill of missing counts.
        photo_ids = None

    try:
        from trr_backend.clients.screenalytics import (
            ScreenalyticsClientError,
            count_people,
            is_screenalytics_configured,
        )
        from trr_backend.repositories.cast_photo_tags import (
            get_tags_by_photo_ids,
            has_manual_tags,
            upsert_cast_photo_tags,
        )

        if not is_screenalytics_configured():
            return auto_counts_attempted, auto_counts_succeeded, auto_counts_failed

        query = (
            db.schema("core")
            .table("cast_photos")
            .select(
                "id, hosted_url, hosted_content_type, url, image_url, thumb_url, "
                "source_page_url, people_names, source, metadata"
            )
            .eq("person_id", person_id)
            .in_("source", candidate_sources)
        )
        if photo_ids:
            query = query.in_("id", photo_ids)
        response = query.execute()

        if hasattr(response, "error") and response.error:
            logger.warning("Auto-count query failed for %s: %s", person_id, response.error)
            return auto_counts_attempted, auto_counts_succeeded, auto_counts_failed

        rows = response.data or []
        if not rows:
            return auto_counts_attempted, auto_counts_succeeded, auto_counts_failed

        tag_rows = get_tags_by_photo_ids(db, [str(row["id"]) for row in rows])
        to_process: list[dict[str, Any]] = []
        for row in rows:
            tag_row = tag_rows.get(str(row["id"]))
            if has_manual_tags(tag_row):
                continue
            if not force_recount and tag_row and tag_row.get("people_count") is not None:
                continue
            image_urls = _pick_autocount_urls(row)
            if not image_urls:
                continue
            to_process.append(
                {
                    "photo_id": str(row["id"]),
                    "image_urls": image_urls,
                    "row": row,
                    "tag_row": tag_row,
                }
            )

        total = len(to_process)
        for idx, item in enumerate(to_process, start=1):
            row = item["row"]
            tag_row = item["tag_row"]
            auto_counts_attempted += 1
            result = None
            last_error: ScreenalyticsClientError | None = None
            for image_url in item["image_urls"]:
                try:
                    result = count_people(image_url)
                    break
                except ScreenalyticsClientError as exc:
                    last_error = exc
            try:
                if result is None:
                    raise last_error or ScreenalyticsClientError("Unable to auto-count image")
                upsert_cast_photo_tags(
                    db,
                    cast_photo_id=item["photo_id"],
                    people_names=tag_row.get("people_names") if tag_row else None,
                    people_ids=tag_row.get("people_ids") if tag_row else None,
                    people_count=result.people_count,
                    people_count_source="auto",
                    detector=result.detector,
                    updated_by_firebase_uid="system:auto",
                )
                crop_payload = _apply_auto_crop_payload(result)
                if crop_payload is not None:
                    existing_meta = dict(row.get("metadata") or {})
                    existing_crop = existing_meta.get("thumbnail_crop")
                    if not (isinstance(existing_crop, dict) and existing_crop.get("mode") == "manual"):
                        existing_meta["thumbnail_crop"] = crop_payload
                        try:
                            db.schema("core").table("cast_photos").update({"metadata": existing_meta}).eq(
                                "id", str(row["id"])
                            ).execute()
                        except Exception as crop_exc:
                            logger.warning(
                                "Failed to store face centroid for %s: %s",
                                row.get("id"),
                                crop_exc,
                            )
                auto_counts_succeeded += 1
            except ScreenalyticsClientError as exc:
                auto_counts_failed += 1
                logger.warning("Auto-count failed for %s: %s", row.get("id"), exc)
            if progress_cb:
                progress_cb(idx, total)
    except Exception as exc:
        logger.exception("Auto-count setup failed for %s: %s", person_id, exc)

    return auto_counts_attempted, auto_counts_succeeded, auto_counts_failed


def _auto_count_media_links(
    db: SupabaseAdminClient,
    person_id: str,
    *,
    force_recount: bool = False,
    progress_cb: Callable[[int, int], None] | None = None,
) -> tuple[int, int, int]:
    attempted = 0
    succeeded = 0
    failed = 0

    try:
        from trr_backend.clients.screenalytics import (
            ScreenalyticsClientError,
            count_people,
            is_screenalytics_configured,
        )
        from trr_backend.repositories.media_links import (
            has_manual_people_tags,
            has_people_count,
        )

        if not is_screenalytics_configured():
            return attempted, succeeded, failed

        rows = _fetch_person_media_link_rows(db, person_id)
        to_process: list[dict[str, Any]] = []
        for row in rows:
            context = row.get("context") if isinstance(row.get("context"), dict) else {}
            if has_manual_people_tags(context):
                continue
            if not force_recount and has_people_count(context):
                continue
            urls = _build_media_link_autocount_urls(row)
            if not urls:
                continue
            to_process.append({"row": row, "urls": urls, "context": dict(context or {})})

        total = len(to_process)
        for idx, item in enumerate(to_process, start=1):
            attempted += 1
            row = item["row"]
            context = item["context"]
            result = None
            last_error: ScreenalyticsClientError | None = None
            for image_url in item["urls"]:
                try:
                    result = count_people(image_url)
                    break
                except ScreenalyticsClientError as exc:
                    last_error = exc
            try:
                if result is None:
                    raise last_error or ScreenalyticsClientError("Unable to auto-count image")
                context["people_count"] = result.people_count
                context["people_count_source"] = "auto"
                context["people_count_detector"] = result.detector
                crop_payload = _apply_auto_crop_payload(result)
                if crop_payload is not None and not _is_manual_thumbnail_crop(context.get("thumbnail_crop")):
                    context["thumbnail_crop"] = crop_payload
                db.schema("core").table("media_links").update(
                    {"context": context, "updated_at": datetime.now(UTC).isoformat()}
                ).eq("id", row["id"]).execute()
                succeeded += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                logger.warning("Auto-count media_link failed for %s: %s", row.get("id"), exc)
            if progress_cb:
                progress_cb(idx, total)
    except Exception as exc:
        logger.exception("Auto-count media_links setup failed for %s: %s", person_id, exc)

    return attempted, succeeded, failed


def _chunked(values: list[str], size: int = 100) -> list[list[str]]:
    return [values[i : i + size] for i in range(0, len(values), size)]


def _enrich_cast_photos_with_episode_metadata(
    db: SupabaseAdminClient,
    photos: list[dict[str, Any]],
) -> tuple[int, int]:
    tagged = 0
    failed = 0
    imdb_ids: list[str] = []
    for row in photos:
        if row.get("source") != "imdb":
            continue
        title_ids = row.get("title_imdb_ids") or []
        if not isinstance(title_ids, list):
            continue
        for imdb_id in title_ids:
            if isinstance(imdb_id, str) and imdb_id.strip():
                imdb_ids.append(imdb_id.strip())

    imdb_ids = list(dict.fromkeys(imdb_ids))
    if not imdb_ids:
        return tagged, failed

    episodes_by_imdb: dict[str, dict[str, Any]] = {}
    for chunk in _chunked(imdb_ids, 100):
        response = (
            db.schema("core")
            .table("episodes")
            .select("id,imdb_episode_id,title,episode_number,season_number,air_date,show_id,show_name")
            .in_("imdb_episode_id", chunk)
            .execute()
        )
        if hasattr(response, "error") and response.error:
            logger.warning("Episode lookup failed: %s", response.error)
            failed += 1
            continue
        for row in response.data or []:
            imdb_episode_id = row.get("imdb_episode_id")
            if imdb_episode_id:
                episodes_by_imdb[str(imdb_episode_id)] = row

    if not episodes_by_imdb:
        return tagged, failed

    for row in photos:
        if row.get("source") != "imdb":
            continue
        title_ids = row.get("title_imdb_ids") or []
        if not isinstance(title_ids, list):
            continue
        episode = None
        for imdb_id in title_ids:
            if imdb_id in episodes_by_imdb:
                episode = episodes_by_imdb[imdb_id]
                break
        if not episode:
            continue

        metadata = dict(row.get("metadata") or {})
        metadata.update(
            {
                "episode_id": episode.get("id"),
                "episode_imdb_id": episode.get("imdb_episode_id"),
                "episode_title": episode.get("title"),
                "episode_number": episode.get("episode_number"),
                "season_number": episode.get("season_number"),
                "episode_air_date": episode.get("air_date"),
                "show_id": episode.get("show_id"),
                "show_name": episode.get("show_name"),
                "source_created_at": episode.get("air_date"),
            }
        )
        row["metadata"] = metadata
        if not row.get("season") and episode.get("season_number"):
            row["season"] = episode.get("season_number")
        tagged += 1
    return tagged, failed


def _apply_show_context_to_photos(
    db: SupabaseAdminClient,
    photos: list[dict[str, Any]],
    *,
    show_id: UUID | None,
    show_name: str | None,
) -> tuple[int, int]:
    tagged = 0
    failed = 0
    if not photos:
        return tagged, failed
    if show_id is None and not (isinstance(show_name, str) and show_name.strip()):
        return tagged, failed

    show_id_str = str(show_id) if show_id is not None else None
    show_name_val = show_name.strip() if isinstance(show_name, str) and show_name.strip() else None

    if show_id_str and not show_name_val:
        resp = db.schema("core").table("shows").select("id,name").eq("id", show_id_str).limit(1).execute()
        if hasattr(resp, "error") and resp.error:
            failed += 1
        elif resp.data:
            show_name_val = str(resp.data[0].get("name") or "").strip() or None

    for row in photos:
        metadata = dict(row.get("metadata") or {})
        before_show_id = metadata.get("show_id")
        before_show_name = metadata.get("show_name")
        if show_id_str:
            metadata.setdefault("show_id", show_id_str)
        if show_name_val:
            metadata.setdefault("show_name", show_name_val)
        row["metadata"] = metadata
        if metadata.get("show_id") != before_show_id or metadata.get("show_name") != before_show_name:
            tagged += 1
    return tagged, failed


def _refresh_tmdb_profile(
    db: SupabaseAdminClient,
    person_id: str,
    *,
    tmdb_person_id: int | None,
) -> None:
    if not tmdb_person_id:
        return
    from trr_backend.integrations.tmdb_person import fetch_tmdb_person_full
    from trr_backend.repositories.cast_tmdb import upsert_cast_tmdb

    person_full = fetch_tmdb_person_full(int(tmdb_person_id))
    if not person_full:
        return
    upsert_cast_tmdb(db, person_full.to_cast_tmdb_row(person_id))


def _normalize_name_for_match(value: str | None) -> str:
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"\(.*?\)", " ", text)
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text)
    return " ".join(text.split()).strip().lower()


_HONORIFIC_NAME_TOKENS = {"dr", "doctor", "mr", "mrs", "ms", "miss", "sir", "lady"}


def _tokenize_name_for_match(value: str | None) -> list[str]:
    normalized = _normalize_name_for_match(value)
    if not normalized:
        return []
    tokens = [token for token in normalized.split() if token]
    while tokens and tokens[0] in _HONORIFIC_NAME_TOKENS:
        tokens.pop(0)
    return tokens


def _names_match(expected: str | None, candidate: str | None) -> bool:
    expected_tokens = _tokenize_name_for_match(expected)
    candidate_tokens = _tokenize_name_for_match(candidate)
    if not expected_tokens or not candidate_tokens:
        return False
    if expected_tokens == candidate_tokens:
        return True
    if len(expected_tokens) == 1 or len(candidate_tokens) == 1:
        return expected_tokens[0] == candidate_tokens[0]
    expected_first = expected_tokens[0]
    expected_last = expected_tokens[-1]
    candidate_first = candidate_tokens[0]
    candidate_last = candidate_tokens[-1]
    if expected_last != candidate_last:
        return False
    if expected_first == candidate_first:
        return True
    if (
        len(expected_first) >= 3
        and len(candidate_first) >= 3
        and (
            expected_first.startswith(candidate_first)
            or candidate_first.startswith(expected_first)
        )
    ):
        return True
    return False


def _name_from_fandom_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    path = parsed.path or ""
    if "/wiki/" not in path:
        return None
    slug = path.split("/wiki/", 1)[1]
    slug = slug.split("/", 1)[0]
    slug = unquote(slug)
    slug = slug.replace("_", " ")
    if slug.lower().endswith(" gallery"):
        slug = slug[: -len(" gallery")]
    return slug.strip() or None


def _fandom_profile_matches_person_name(
    expected_name: str,
    cast_fandom: dict[str, Any],
    *,
    page_url: str | None = None,
) -> bool:
    page_owner = _name_from_fandom_url(page_url)
    if page_owner and not _names_match(expected_name, page_owner):
        return False
    candidates = [
        cast_fandom.get("full_name"),
        cast_fandom.get("page_title"),
        page_owner,
    ]
    return any(_names_match(expected_name, cand if isinstance(cand, str) else None) for cand in candidates)


def _refresh_fandom_profile(
    db: SupabaseAdminClient,
    person_id: str,
    *,
    person_name: str | None,
) -> None:
    if not (isinstance(person_name, str) and person_name.strip()):
        return
    from trr_backend.ingestion.fandom_person_scraper import fetch_fandom_person_html, parse_fandom_person_html
    from trr_backend.integrations.fandom import build_real_housewives_wiki_url_from_name, search_real_housewives_wiki
    from trr_backend.repositories.cast_fandom import upsert_cast_fandom

    candidates: list[str] = []
    search_url = search_real_housewives_wiki(person_name)
    if search_url:
        candidates.append(search_url)
    fallback_url = build_real_housewives_wiki_url_from_name(person_name)
    if fallback_url and fallback_url not in candidates:
        candidates.append(fallback_url)

    for url in candidates:
        if not _names_match(person_name, _name_from_fandom_url(url)):
            continue
        html, final_url = fetch_fandom_person_html(url)
        if not html:
            continue
        cast_fandom, _photos = parse_fandom_person_html(html, source_url=final_url)
        if not isinstance(cast_fandom, dict) or not cast_fandom:
            continue
        if not _fandom_profile_matches_person_name(person_name, cast_fandom, page_url=final_url):
            continue
        cast_fandom = dict(cast_fandom)
        cast_fandom["person_id"] = person_id
        upsert_cast_fandom(db, cast_fandom)
        return


def _detect_text_overlay_cast_photos(
    db: SupabaseAdminClient,
    person_id: str,
    sources: list[SourceType],
    *,
    photo_ids: list[str] | None = None,
    progress_cb: Callable[[int, int], None] | None = None,
    reason_counts: dict[str, int] | None = None,
) -> tuple[int, int, int, int]:
    """
    Best-effort "word id" detection for cast_photos rows.

    Returns (attempted, succeeded, unknown, failed).
    """
    attempted = 0
    succeeded = 0
    unknown = 0
    failed = 0

    candidate_sources = [s for s in sources if s in ALL_SOURCES]
    if not candidate_sources:
        return attempted, succeeded, unknown, failed
    if photo_ids is not None and not photo_ids:
        photo_ids = None

    try:
        from trr_backend.vision.text_overlay import (
            TextOverlayDetectionError,
            classify_text_overlay_failure_reason,
            detect_and_update_cast_photo_text_overlay,
            is_text_overlay_detection_configured,
        )

        if not is_text_overlay_detection_configured():
            return attempted, succeeded, unknown, failed

        query = (
            db.schema("core")
            .table("cast_photos")
            .select("id, metadata, source")
            .eq("person_id", person_id)
            .in_("source", candidate_sources)
        )
        if photo_ids:
            query = query.in_("id", photo_ids)
        response = query.execute()
        if hasattr(response, "error") and response.error:
            logger.warning("Word detection query failed for %s: %s", person_id, response.error)
            return attempted, succeeded, unknown, failed

        rows = response.data or []
        to_process: list[str] = []
        for row in rows:
            meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            if "has_text_overlay" in (meta or {}):
                continue
            rid = row.get("id")
            if rid:
                to_process.append(str(rid))

        total = len(to_process)
        for idx, photo_id in enumerate(to_process, start=1):
            attempted += 1
            try:
                result = detect_and_update_cast_photo_text_overlay(db, photo_id, force=False)
                if result.status == "unknown":
                    unknown += 1
                    reason = result.reason_code
                    if reason_counts is not None and isinstance(reason, str) and reason:
                        reason_counts[reason] = reason_counts.get(reason, 0) + 1
                else:
                    succeeded += 1
            except TextOverlayDetectionError as exc:
                failed += 1
                if reason_counts is not None:
                    reason = classify_text_overlay_failure_reason(exc)
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
                logger.warning("Word detection failed photo_id=%s error=%s", photo_id, exc)
            except Exception as exc:  # noqa: BLE001
                failed += 1
                if reason_counts is not None:
                    reason_counts["gemini_request_failed"] = reason_counts.get("gemini_request_failed", 0) + 1
                logger.warning("Word detection failed photo_id=%s error=%s", photo_id, exc)

            if progress_cb:
                progress_cb(idx, total)
    except Exception as exc:
        logger.exception("Word detection setup failed for %s: %s", person_id, exc)

    return attempted, succeeded, unknown, failed


def _detect_text_overlay_media_links(
    db: SupabaseAdminClient,
    person_id: str,
    *,
    progress_cb: Callable[[int, int], None] | None = None,
    reason_counts: dict[str, int] | None = None,
) -> tuple[int, int, int, int]:
    attempted = 0
    succeeded = 0
    unknown = 0
    failed = 0

    try:
        from trr_backend.vision.text_overlay import (
            TextOverlayDetectionError,
            classify_text_overlay_failure_reason,
            detect_and_update_media_asset_text_overlay,
            is_text_overlay_detection_configured,
        )

        if not is_text_overlay_detection_configured():
            return attempted, succeeded, unknown, failed

        rows = _fetch_person_media_link_rows(db, person_id)
        to_process: list[str] = []
        seen_asset_ids: set[str] = set()
        for row in rows:
            asset_id = str(row.get("media_asset_id") or "")
            if not asset_id or asset_id in seen_asset_ids:
                continue
            seen_asset_ids.add(asset_id)
            metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            if "has_text_overlay" in metadata:
                continue
            to_process.append(asset_id)

        total = len(to_process)
        for idx, asset_id in enumerate(to_process, start=1):
            attempted += 1
            try:
                result = detect_and_update_media_asset_text_overlay(db, asset_id, force=False)
                if result.status == "unknown":
                    unknown += 1
                    reason = result.reason_code
                    if reason_counts is not None and isinstance(reason, str) and reason:
                        reason_counts[reason] = reason_counts.get(reason, 0) + 1
                else:
                    succeeded += 1
            except TextOverlayDetectionError as exc:
                failed += 1
                if reason_counts is not None:
                    reason = classify_text_overlay_failure_reason(exc)
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
                logger.warning("Word detection failed media_asset_id=%s error=%s", asset_id, exc)
            except Exception as exc:  # noqa: BLE001
                failed += 1
                if reason_counts is not None:
                    reason_counts["gemini_request_failed"] = reason_counts.get("gemini_request_failed", 0) + 1
                logger.warning("Word detection failed media_asset_id=%s error=%s", asset_id, exc)
            if progress_cb:
                progress_cb(idx, total)
    except Exception as exc:
        logger.exception("Word detection media links setup failed for %s: %s", person_id, exc)

    return attempted, succeeded, unknown, failed


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
    requested_sources = request.sources or ALL_SOURCES
    sources, fandom_skipped = _apply_show_source_policy(db, request.show_id, list(requested_sources))
    errors: list[str] = []
    if fandom_skipped:
        errors.append("Fandom sources skipped for non-Real Housewives show context.")

    # 1.5 Refresh person profiles (best-effort)
    try:
        _refresh_tmdb_profile(db, person_id_str, tmdb_person_id=tmdb_person_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("TMDb profile refresh failed for %s: %s", person_id, exc)
        errors.append(f"TMDb profile: {exc}")
    if "fandom" in sources or "fandom-gallery" in sources:
        try:
            _refresh_fandom_profile(db, person_id_str, person_name=person_name)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Fandom profile refresh failed for %s: %s", person_id, exc)
            errors.append(f"Fandom profile: {exc}")

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
        episode_metadata_tagged = 0
        show_context_tagged = 0
        metadata_enrichment_failed = 1
    else:
        episode_metadata_tagged = 0
        show_context_tagged = 0
        metadata_enrichment_failed = 0
        try:
            episode_metadata_tagged, episode_metadata_failed = _enrich_cast_photos_with_episode_metadata(db, photos)
            metadata_enrichment_failed += episode_metadata_failed
        except Exception as exc:
            logger.warning("Episode metadata enrichment failed for %s: %s", person_id, exc)
            metadata_enrichment_failed += 1
        try:
            show_context_tagged, show_context_failed = _apply_show_context_to_photos(
                db,
                photos,
                show_id=request.show_id,
                show_name=request.show_name,
            )
            metadata_enrichment_failed += show_context_failed
        except Exception as exc:
            logger.warning("Show context tagging failed for %s: %s", person_id, exc)
            metadata_enrichment_failed += 1

    # 3. Upsert to database
    photos_upserted = 0
    upserted_photo_ids: list[str] = []
    if photos:
        imdb_photos = [p for p in photos if p.get("source") == "imdb"]
        other_photos = [p for p in photos if p.get("source") != "imdb"]
        try:
            if imdb_photos:
                upserted = upsert_cast_photos(db, imdb_photos, dedupe_on="source_image_id")
                photos_upserted += len(upserted)
                upserted_photo_ids.extend([str(row["id"]) for row in upserted if row.get("id")])
            if other_photos:
                upserted = upsert_cast_photos(db, other_photos, dedupe_on="image_url_canonical")
                photos_upserted += len(upserted)
                upserted_photo_ids.extend([str(row["id"]) for row in upserted if row.get("id")])
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

    # 4.5 Auto-count people for newly upserted TMDb/Fandom photos (only when no manual tags)
    auto_counts_attempted_cast, auto_counts_succeeded_cast, auto_counts_failed_cast = _auto_count_cast_photos(
        db,
        person_id_str,
        sources,
        photo_ids=None,
    )
    auto_counts_attempted_media, auto_counts_succeeded_media, auto_counts_failed_media = _auto_count_media_links(
        db,
        person_id_str,
        force_recount=False,
    )
    auto_counts_attempted = auto_counts_attempted_cast + auto_counts_attempted_media
    auto_counts_succeeded = auto_counts_succeeded_cast + auto_counts_succeeded_media
    auto_counts_failed = auto_counts_failed_cast + auto_counts_failed_media

    # 4.6 Word ID / text overlay detection (best-effort)
    text_overlay_reason_counts: dict[str, int] = {}
    (
        text_overlay_attempted_cast,
        text_overlay_succeeded_cast,
        text_overlay_unknown_cast,
        text_overlay_failed_cast,
    ) = _detect_text_overlay_cast_photos(
        db,
        person_id_str,
        sources,
        photo_ids=None,
        reason_counts=text_overlay_reason_counts,
    )
    (
        text_overlay_attempted_media,
        text_overlay_succeeded_media,
        text_overlay_unknown_media,
        text_overlay_failed_media,
    ) = _detect_text_overlay_media_links(
        db,
        person_id_str,
        reason_counts=text_overlay_reason_counts,
    )
    text_overlay_attempted = text_overlay_attempted_cast + text_overlay_attempted_media
    text_overlay_succeeded = text_overlay_succeeded_cast + text_overlay_succeeded_media
    text_overlay_unknown = text_overlay_unknown_cast + text_overlay_unknown_media
    text_overlay_failed = text_overlay_failed_cast + text_overlay_failed_media
    text_overlay_failure_reasons = {
        reason: int(text_overlay_reason_counts.get(reason, 0)) for reason in TEXT_OVERLAY_FAILURE_REASONS
    }

    # 4.7 Center/crop thumbnails for non-manual rows (best-effort)
    centering_attempted, centering_succeeded, centering_failed, centering_skipped_manual = (
        _recenter_person_gallery_images(
            db,
            person_id_str,
            sources,
            photo_ids=None,
            force=False,
        )
    )

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
        auto_counts_attempted=auto_counts_attempted,
        auto_counts_succeeded=auto_counts_succeeded,
        auto_counts_failed=auto_counts_failed,
        text_overlay_attempted=text_overlay_attempted,
        text_overlay_succeeded=text_overlay_succeeded,
        text_overlay_unknown=text_overlay_unknown,
        text_overlay_failed=text_overlay_failed,
        text_overlay_failure_reasons=text_overlay_failure_reasons,
        episode_metadata_tagged=episode_metadata_tagged,
        show_context_tagged=show_context_tagged,
        metadata_enrichment_failed=metadata_enrichment_failed,
        centering_attempted=centering_attempted,
        centering_succeeded=centering_succeeded,
        centering_failed=centering_failed,
        centering_skipped_manual=centering_skipped_manual,
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
    from trr_backend.ingestion.cast_photo_sources import (
        fetch_fandom_gallery_cast_photos,
        fetch_fandom_person_cast_photos,
        fetch_imdb_cast_photos,
        fetch_tmdb_cast_photos,
    )
    from trr_backend.media.s3_mirror import mirror_cast_photo_row
    from trr_backend.repositories.cast_photos import (
        fetch_cast_photos_missing_hosted,
        update_cast_photo_hosted_fields,
        upsert_cast_photos,
    )

    request = request or RefreshImagesRequest()
    person_id_str = str(person_id)
    run_id = f"refresh-{person_id_str}-{int(datetime.now(UTC).timestamp())}"

    async def event_generator() -> AsyncGenerator[str, None]:
        errors: list[str] = []
        upserted_photo_ids: list[str] = []
        text_overlay_reason_counts: dict[str, int] = dict.fromkeys(TEXT_OVERLAY_FAILURE_REASONS, 0)
        episode_metadata_tagged = 0
        show_context_tagged = 0
        metadata_enrichment_failed = 0

        def progress(payload: dict[str, Any]) -> str:
            return f"event: progress\ndata: {json.dumps({'run_id': run_id, **payload})}\n\n"

        def error_event(*, stage: str, error: str, detail: str | None = None) -> str:
            payload: dict[str, Any] = {"run_id": run_id, "stage": stage, "error": error}
            if detail:
                payload["detail"] = detail
            return f"event: error\ndata: {json.dumps(payload)}\n\n"

        # 1. Get person
        try:
            person = _get_person_details(db, person_id_str)
            if not person:
                yield error_event(stage="setup", error="Person not found")
                return

            external_ids = person.get("external_ids") or {}
            imdb_person_id = _extract_imdb_id(external_ids)
            tmdb_person_id = _get_tmdb_id(db, person_id_str, external_ids)
            person_name = person.get("full_name")
            requested_sources = request.sources or ALL_SOURCES
            sources, fandom_skipped = _apply_show_source_policy(
                db, request.show_id, list(requested_sources)
            )
            if fandom_skipped:
                errors.append("Fandom sources skipped for non-Real Housewives show context.")
        except Exception as exc:  # noqa: BLE001
            logger.exception("Refresh stream setup failed for %s: %s", person_id_str, exc)
            yield error_event(stage="setup", error="Failed to initialize refresh", detail=str(exc))
            return

        # 1.5 Refresh person profiles (best-effort)
        yield progress({"stage": "tmdb_profile", "message": "Syncing TMDb profile..."})
        try:
            _refresh_tmdb_profile(db, person_id_str, tmdb_person_id=tmdb_person_id)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"TMDb profile: {exc}")

        if "fandom" in sources or "fandom-gallery" in sources:
            yield progress({"stage": "fandom_profile", "message": "Syncing Fandom profile..."})
            try:
                _refresh_fandom_profile(db, person_id_str, person_name=person_name)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Fandom profile: {exc}")
        else:
            yield progress(
                {
                    "stage": "fandom_profile",
                    "message": "Skipping Fandom profile (non-Real Housewives show context).",
                }
            )

        # 2. Fetch per-source so the UI can show live stage updates.
        enabled = set(sources)
        fetch_steps: list[tuple[str, str]] = []
        if "imdb" in enabled and imdb_person_id:
            fetch_steps.append(("sync_imdb", "IMDb"))
        if "tmdb" in enabled and tmdb_person_id:
            fetch_steps.append(("sync_tmdb", "TMDb"))
        if "fandom" in enabled and person_name:
            fetch_steps.append(("sync_fandom", "Fandom"))
        if "fandom-gallery" in enabled and person_name:
            fetch_steps.append(("sync_fandom_gallery", "Fandom Gallery"))

        total_sources = len(fetch_steps)
        processed_sources = 0
        photos: list[dict[str, Any]] = []

        for stage, label in fetch_steps:
            yield progress(
                {
                    "stage": stage,
                    "message": f"Syncing {label}...",
                    "current": processed_sources,
                    "total": total_sources,
                }
            )
            rows: list[dict[str, Any]] = []
            try:
                if stage == "sync_imdb":
                    rows = fetch_imdb_cast_photos(
                        imdb_person_id,
                        person_id_str,
                        limit=request.limit_per_source,
                        session=None,
                        verbose=False,
                    )
                elif stage == "sync_tmdb":
                    rows = fetch_tmdb_cast_photos(
                        int(tmdb_person_id),
                        person_id_str,
                        imdb_person_id=imdb_person_id,
                        limit=request.limit_per_source,
                        verbose=False,
                    )
                elif stage == "sync_fandom":
                    rows = fetch_fandom_person_cast_photos(
                        str(person_name),
                        person_id_str,
                        imdb_person_id=imdb_person_id,
                        limit=request.limit_per_source,
                        verbose=False,
                    )
                else:
                    rows = fetch_fandom_gallery_cast_photos(
                        str(person_name),
                        person_id_str,
                        imdb_person_id=imdb_person_id,
                        limit=request.limit_per_source,
                        verbose=False,
                    )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{label}: {exc}")
                rows = []
            photos.extend(rows)
            processed_sources += 1
            yield progress(
                {
                    "stage": stage,
                    "message": f"Synced {label} ({len(rows)} photos).",
                    "current": processed_sources,
                    "total": total_sources,
                }
            )

        yield progress(
            {
                "stage": "metadata_enrichment",
                "message": "Tagging episode metadata...",
                "current": 0,
                "total": 2,
            }
        )
        try:
            episode_metadata_tagged, episode_failed = _enrich_cast_photos_with_episode_metadata(db, photos)
            metadata_enrichment_failed += episode_failed
        except Exception as exc:
            metadata_enrichment_failed += 1
            errors.append(f"Metadata enrichment (episode): {exc}")

        yield progress(
            {
                "stage": "metadata_enrichment",
                "message": "Applying show context...",
                "current": 1,
                "total": 2,
            }
        )
        try:
            show_context_tagged, show_failed = _apply_show_context_to_photos(
                db,
                photos,
                show_id=request.show_id,
                show_name=request.show_name,
            )
            metadata_enrichment_failed += show_failed
        except Exception as exc:
            metadata_enrichment_failed += 1
            errors.append(f"Metadata enrichment (show context): {exc}")

        yield progress(
            {
                "stage": "metadata_enrichment",
                "message": "Metadata enrichment complete.",
                "current": 2,
                "total": 2,
            }
        )

        yield progress(
            {
                "stage": "fetching",
                "message": f"Fetched {len(photos)} photos.",
                "current": len(photos),
                "total": max(1, len(photos)),
            }
        )

        # 3. Upsert
        photos_upserted = 0
        if photos:
            yield progress(
                {
                    "stage": "upserting",
                    "message": "Upserting photos...",
                    "current": 0,
                    "total": len(photos),
                }
            )
            imdb_photos = [p for p in photos if p.get("source") == "imdb"]
            other_photos = [p for p in photos if p.get("source") != "imdb"]
            try:
                if imdb_photos:
                    upserted = upsert_cast_photos(db, imdb_photos, dedupe_on="source_image_id")
                    photos_upserted += len(upserted)
                    upserted_photo_ids.extend([str(row["id"]) for row in upserted if row.get("id")])
                if other_photos:
                    upserted = upsert_cast_photos(db, other_photos, dedupe_on="image_url_canonical")
                    photos_upserted += len(upserted)
                    upserted_photo_ids.extend([str(row["id"]) for row in upserted if row.get("id")])
            except Exception as exc:
                errors.append(str(exc))
            yield progress(
                {
                    "stage": "upserting",
                    "message": "Upsert complete.",
                    "current": photos_upserted,
                    "total": len(photos),
                }
            )
        else:
            yield progress({"stage": "upserting", "message": "No photos to upsert.", "current": 0, "total": 0})

        # 4. Mirror
        photos_mirrored, photos_failed = 0, 0
        if not request.skip_mirror:
            try:
                from trr_backend.media.s3_mirror import get_cdn_base_url

                cdn_url = None if request.force_mirror else get_cdn_base_url()
                # When force_mirror=True, include photos that already have hosted_url so they get re-uploaded
                rows = fetch_cast_photos_missing_hosted(
                    db, person_ids=[person_id_str], cdn_base_url=cdn_url, include_hosted=request.force_mirror
                )
                total_rows = len(rows)
                yield progress(
                    {
                        "stage": "mirroring",
                        "message": "Mirroring to S3...",
                        "current": 0,
                        "total": total_rows,
                    }
                )
                for idx, row in enumerate(rows, start=1):
                    if not row.get("imdb_person_id") and imdb_person_id:
                        row["imdb_person_id"] = imdb_person_id
                    try:
                        patch = mirror_cast_photo_row(row, force=request.force_mirror)
                        if patch:
                            update_cast_photo_hosted_fields(db, str(row["id"]), patch)
                            photos_mirrored += 1
                    except Exception:
                        photos_failed += 1
                    if idx <= 20 or idx % 5 == 0 or idx == total_rows:
                        data = {
                            "stage": "mirroring",
                            "message": "Mirroring to S3...",
                            "current": idx,
                            "total": total_rows,
                        }
                        yield progress(data)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Mirror: {exc}")
                yield progress(
                    {
                        "stage": "mirroring",
                        "message": f"Mirroring failed: {exc}",
                        "current": 0,
                        "total": 0,
                    }
                )
        else:
            yield progress({"stage": "mirroring", "message": "Skipping S3 mirroring.", "current": 0, "total": 0})

        # 5. Prune
        photos_pruned = 0
        if not request.skip_mirror and not request.skip_prune:
            yield progress({"stage": "pruning", "message": "Pruning orphaned S3 objects..."})
            photos_pruned = _prune_person_s3_objects(db, imdb_person_id or person_id_str)
            yield progress(
                {
                    "stage": "pruning",
                    "message": f"Pruned {photos_pruned} objects.",
                    "current": 1,
                    "total": 1,
                }
            )

        # 5.5 Auto-count people (best-effort; skips manual tags/counts)
        auto_counts_attempted = 0
        auto_counts_succeeded = 0
        auto_counts_failed = 0
        try:
            from trr_backend.clients.screenalytics import (
                ScreenalyticsClientError,
                count_people,
                is_screenalytics_configured,
            )
            from trr_backend.repositories.cast_photo_tags import (
                get_tags_by_photo_ids,
                has_manual_tags,
                upsert_cast_photo_tags,
            )
            from trr_backend.repositories.media_links import (
                has_manual_people_tags,
                has_people_count,
            )

            if is_screenalytics_configured():
                candidate_sources = [s for s in sources if s in ALL_SOURCES]
                cast_rows = (
                    db.schema("core")
                    .table("cast_photos")
                    .select(
                        "id, hosted_url, hosted_content_type, url, image_url, thumb_url, "
                        "source_page_url, people_names, source, metadata"
                    )
                    .eq("person_id", person_id_str)
                    .in_("source", candidate_sources)
                    .execute()
                    .data
                    or []
                )
                tag_rows = get_tags_by_photo_ids(db, [str(row["id"]) for row in cast_rows if row.get("id")])
                media_rows = _fetch_person_media_link_rows(db, person_id_str)

                to_process: list[dict[str, Any]] = []
                for row in cast_rows:
                    tag_row = tag_rows.get(str(row["id"]))
                    if has_manual_tags(tag_row):
                        continue
                    if tag_row and tag_row.get("people_count") is not None:
                        continue
                    urls = _pick_autocount_urls(row)
                    if not urls:
                        continue
                    to_process.append(
                        {
                            "origin": "cast_photos",
                            "id": str(row["id"]),
                            "urls": urls,
                            "tag_row": tag_row,
                            "row": row,
                        }
                    )

                for row in media_rows:
                    context = row.get("context") if isinstance(row.get("context"), dict) else {}
                    if has_manual_people_tags(context):
                        continue
                    if has_people_count(context):
                        continue
                    urls = _build_media_link_autocount_urls(row)
                    if not urls:
                        continue
                    to_process.append(
                        {
                            "origin": "media_links",
                            "id": str(row["id"]),
                            "urls": urls,
                            "context": dict(context or {}),
                        }
                    )

                total_to_count = len(to_process)
                yield progress(
                    {
                        "stage": "auto_count",
                        "message": "Auto-counting people in images...",
                        "current": 0,
                        "total": total_to_count,
                    }
                )
                for idx, entry in enumerate(to_process, start=1):
                    auto_counts_attempted += 1
                    result = None
                    last_error: ScreenalyticsClientError | None = None
                    for url in entry["urls"]:
                        try:
                            result = count_people(url)
                            break
                        except ScreenalyticsClientError as exc:
                            last_error = exc
                    try:
                        if result is None:
                            raise last_error or ScreenalyticsClientError("Unable to auto-count image")
                        if entry["origin"] == "cast_photos":
                            tag_row = entry.get("tag_row")
                            upsert_cast_photo_tags(
                                db,
                                cast_photo_id=entry["id"],
                                people_names=tag_row.get("people_names") if tag_row else None,
                                people_ids=tag_row.get("people_ids") if tag_row else None,
                                people_count=result.people_count,
                                people_count_source="auto",
                                detector=result.detector,
                                updated_by_firebase_uid="system:auto",
                            )
                            crop_payload = _apply_auto_crop_payload(result)
                            if crop_payload is not None:
                                metadata = dict(entry["row"].get("metadata") or {})
                                if not _is_manual_thumbnail_crop(metadata.get("thumbnail_crop")):
                                    metadata["thumbnail_crop"] = crop_payload
                                    db.schema("core").table("cast_photos").update({"metadata": metadata}).eq(
                                        "id", entry["id"]
                                    ).execute()
                        else:
                            context = dict(entry.get("context") or {})
                            context["people_count"] = result.people_count
                            context["people_count_source"] = "auto"
                            context["people_count_detector"] = result.detector
                            crop_payload = _apply_auto_crop_payload(result)
                            if crop_payload is not None and not _is_manual_thumbnail_crop(
                                context.get("thumbnail_crop")
                            ):
                                context["thumbnail_crop"] = crop_payload
                            db.schema("core").table("media_links").update(
                                {"context": context, "updated_at": datetime.now(UTC).isoformat()}
                            ).eq("id", entry["id"]).execute()
                        auto_counts_succeeded += 1
                    except Exception as exc:  # noqa: BLE001
                        auto_counts_failed += 1
                        errors.append(f"Auto-count {entry['id']}: {exc}")

                    if idx <= 20 or idx % 5 == 0 or idx == total_to_count:
                        yield progress(
                            {
                                "stage": "auto_count",
                                "message": "Auto-counting people in images...",
                                "current": idx,
                                "total": total_to_count,
                            }
                        )
            else:
                yield progress(
                    {
                        "stage": "auto_count",
                        "message": "Skipping auto-count (not configured).",
                        "current": 0,
                        "total": 0,
                    }
                )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"Auto-count: {exc}")

        # 5.6 Word ID / text overlay detection (best-effort)
        text_overlay_attempted = 0
        text_overlay_succeeded = 0
        text_overlay_unknown = 0
        text_overlay_failed = 0
        try:
            from trr_backend.vision.text_overlay import (
                TextOverlayDetectionError,
                classify_text_overlay_failure_reason,
                detect_and_update_cast_photo_text_overlay,
                detect_and_update_media_asset_text_overlay,
                is_text_overlay_detection_configured,
            )

            if is_text_overlay_detection_configured():
                cast_rows = (
                    db.schema("core")
                    .table("cast_photos")
                    .select("id, metadata, source")
                    .eq("person_id", person_id_str)
                    .in_("source", [s for s in sources if s in ALL_SOURCES])
                    .execute()
                    .data
                    or []
                )
                media_rows = _fetch_person_media_link_rows(db, person_id_str)

                to_process: list[dict[str, str]] = []
                for row in cast_rows:
                    meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
                    if "has_text_overlay" in (meta or {}):
                        continue
                    rid = row.get("id")
                    if rid:
                        to_process.append({"origin": "cast_photos", "id": str(rid)})

                seen_media_assets: set[str] = set()
                for row in media_rows:
                    asset_id = str(row.get("media_asset_id") or "")
                    if not asset_id or asset_id in seen_media_assets:
                        continue
                    seen_media_assets.add(asset_id)
                    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
                    if "has_text_overlay" in metadata:
                        continue
                    to_process.append({"origin": "media_links", "id": asset_id})

                total_text = len(to_process)
                yield progress(
                    {
                        "stage": "word_id",
                        "message": "Detecting words/text overlays...",
                        "current": 0,
                        "total": total_text,
                    }
                )
                for idx, item in enumerate(to_process, start=1):
                    text_overlay_attempted += 1
                    try:
                        if item["origin"] == "cast_photos":
                            result = detect_and_update_cast_photo_text_overlay(db, item["id"], force=False)
                        else:
                            result = detect_and_update_media_asset_text_overlay(db, item["id"], force=False)
                        if result.status == "unknown":
                            text_overlay_unknown += 1
                            reason = result.reason_code or "gemini_request_failed"
                            text_overlay_reason_counts[reason] = text_overlay_reason_counts.get(reason, 0) + 1
                        else:
                            text_overlay_succeeded += 1
                    except TextOverlayDetectionError as exc:
                        text_overlay_failed += 1
                        reason = classify_text_overlay_failure_reason(exc)
                        text_overlay_reason_counts[reason] = text_overlay_reason_counts.get(reason, 0) + 1
                        errors.append(f"Word ID {item['id']}: {exc}")
                    except Exception as exc:  # noqa: BLE001
                        text_overlay_failed += 1
                        text_overlay_reason_counts["gemini_request_failed"] = (
                            text_overlay_reason_counts.get("gemini_request_failed", 0) + 1
                        )
                        errors.append(f"Word ID {item['id']}: {exc}")

                    if idx <= 20 or idx % 5 == 0 or idx == total_text:
                        yield progress(
                            {
                                "stage": "word_id",
                                "message": "Detecting words/text overlays...",
                                "current": idx,
                                "total": total_text,
                            }
                        )
            else:
                yield progress(
                    {
                        "stage": "word_id",
                        "message": "Skipping word detection (not configured).",
                        "current": 0,
                        "total": 0,
                    }
                )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"Word ID: {exc}")

        # 5.7 Centering/cropping stage (best-effort, non-manual only)
        centering_attempted = 0
        centering_succeeded = 0
        centering_failed = 0
        centering_skipped_manual = 0
        try:
            from trr_backend.clients.screenalytics import (
                ScreenalyticsClientError,
                count_people,
                is_screenalytics_configured,
            )

            if is_screenalytics_configured():
                candidate_sources = [s for s in sources if s in ALL_SOURCES]
                cast_rows = (
                    db.schema("core")
                    .table("cast_photos")
                    .select("id, hosted_url, url, image_url, thumb_url, source_page_url, source, metadata")
                    .eq("person_id", person_id_str)
                    .in_("source", candidate_sources)
                    .execute()
                    .data
                    or []
                )
                media_rows = _fetch_person_media_link_rows(db, person_id_str)

                to_process_crop: list[dict[str, Any]] = []
                for row in cast_rows:
                    metadata = dict(row.get("metadata") or {})
                    existing_crop = metadata.get("thumbnail_crop")
                    if _is_manual_thumbnail_crop(existing_crop):
                        centering_skipped_manual += 1
                        continue
                    if not _should_recenter_auto_crop(existing_crop, force=False):
                        continue
                    urls = _pick_autocount_urls(row)
                    if not urls:
                        continue
                    to_process_crop.append(
                        {
                            "origin": "cast_photos",
                            "id": str(row["id"]),
                            "urls": urls,
                            "metadata": metadata,
                        }
                    )

                for row in media_rows:
                    context = dict(row.get("context") or {})
                    existing_crop = context.get("thumbnail_crop")
                    if _is_manual_thumbnail_crop(existing_crop):
                        centering_skipped_manual += 1
                        continue
                    if not _should_recenter_auto_crop(existing_crop, force=False):
                        continue
                    urls = _build_media_link_autocount_urls(row)
                    if not urls:
                        continue
                    to_process_crop.append(
                        {
                            "origin": "media_links",
                            "id": str(row["id"]),
                            "urls": urls,
                            "context": context,
                        }
                    )

                total_crop = len(to_process_crop)
                yield progress(
                    {
                        "stage": "centering_cropping",
                        "message": "Centering/cropping thumbnails...",
                        "current": 0,
                        "total": total_crop,
                    }
                )
                for idx, entry in enumerate(to_process_crop, start=1):
                    centering_attempted += 1
                    result = None
                    last_error: ScreenalyticsClientError | None = None
                    for url in entry["urls"]:
                        try:
                            result = count_people(url)
                            break
                        except ScreenalyticsClientError as exc:
                            last_error = exc
                    try:
                        if result is None:
                            raise last_error or ScreenalyticsClientError("Unable to center/crop image")
                        crop_payload = _apply_auto_crop_payload(result)
                        if crop_payload is None:
                            raise ScreenalyticsClientError("No detections available for centering/cropping")
                        if entry["origin"] == "cast_photos":
                            metadata = dict(entry["metadata"] or {})
                            metadata["thumbnail_crop"] = crop_payload
                            db.schema("core").table("cast_photos").update({"metadata": metadata}).eq(
                                "id", entry["id"]
                            ).execute()
                        else:
                            context = dict(entry["context"] or {})
                            context["thumbnail_crop"] = crop_payload
                            db.schema("core").table("media_links").update(
                                {
                                    "context": context,
                                    "updated_at": datetime.now(UTC).isoformat(),
                                }
                            ).eq("id", entry["id"]).execute()
                        centering_succeeded += 1
                    except Exception as exc:  # noqa: BLE001
                        centering_failed += 1
                        errors.append(f"Centering {entry['id']}: {exc}")

                    if idx <= 20 or idx % 5 == 0 or idx == total_crop:
                        yield progress(
                            {
                                "stage": "centering_cropping",
                                "message": "Centering/cropping thumbnails...",
                                "current": idx,
                                "total": total_crop,
                            }
                        )
            else:
                yield progress(
                    {
                        "stage": "centering_cropping",
                        "message": "Skipping centering/cropping (not configured).",
                        "current": 0,
                        "total": 0,
                    }
                )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"Centering/Cropping: {exc}")

        # 6. Complete
        complete_data = {
            "run_id": run_id,
            "person_id": person_id_str,
            "photos_fetched": len(photos),
            "photos_upserted": photos_upserted,
            "photos_mirrored": photos_mirrored,
            "photos_failed": photos_failed,
            "photos_pruned": photos_pruned,
            "episode_metadata_tagged": episode_metadata_tagged,
            "show_context_tagged": show_context_tagged,
            "metadata_enrichment_failed": metadata_enrichment_failed,
            "auto_counts_attempted": auto_counts_attempted,
            "auto_counts_succeeded": auto_counts_succeeded,
            "auto_counts_failed": auto_counts_failed,
            "text_overlay_attempted": text_overlay_attempted,
            "text_overlay_succeeded": text_overlay_succeeded,
            "text_overlay_unknown": text_overlay_unknown,
            "text_overlay_failed": text_overlay_failed,
            "text_overlay_failure_reasons": text_overlay_reason_counts,
            "centering_attempted": centering_attempted,
            "centering_succeeded": centering_succeeded,
            "centering_failed": centering_failed,
            "centering_skipped_manual": centering_skipped_manual,
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


@router.post("/{person_id}/reprocess-images/stream")
async def reprocess_person_images_stream(
    person_id: UUID,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> StreamingResponse:
    """Re-run counting, text-ID, and face-crop on existing photos (no sync/mirror)."""
    person_id_str = str(person_id)
    run_id = f"reprocess-{person_id_str}-{int(datetime.now(UTC).timestamp())}"

    async def event_generator() -> AsyncGenerator[str, None]:
        errors: list[str] = []
        text_overlay_reason_counts: dict[str, int] = dict.fromkeys(TEXT_OVERLAY_FAILURE_REASONS, 0)

        def progress(payload: dict[str, Any]) -> str:
            return f"event: progress\ndata: {json.dumps({'run_id': run_id, **payload})}\n\n"

        def error_event(*, stage: str, error: str, detail: str | None = None) -> str:
            payload: dict[str, Any] = {"run_id": run_id, "stage": stage, "error": error}
            if detail:
                payload["detail"] = detail
            return f"event: error\ndata: {json.dumps(payload)}\n\n"

        # Verify person exists
        person = _get_person_details(db, person_id_str)
        if not person:
            yield error_event(stage="setup", error="Person not found")
            return

        sources: list[SourceType] = list(ALL_SOURCES)

        # ---------- Auto-count (cast_photos + media_links) ----------
        yield progress(
            {
                "stage": "auto_count",
                "message": "Auto-counting people in images...",
                "current": None,
                "total": None,
            }
        )

        ac_cast, sc_cast, fc_cast = _auto_count_cast_photos(
            db,
            person_id_str,
            sources,
            force_recount=True,
        )
        ac_media, sc_media, fc_media = _auto_count_media_links(
            db,
            person_id_str,
            force_recount=True,
        )
        auto_counts_attempted = ac_cast + ac_media
        auto_counts_succeeded = sc_cast + sc_media
        auto_counts_failed = fc_cast + fc_media

        yield progress(
            {
                "stage": "auto_count",
                "message": f"Counted {auto_counts_succeeded} images ({auto_counts_failed} failed).",
                "current": auto_counts_attempted,
                "total": auto_counts_attempted,
            }
        )

        # ---------- Word ID / text overlay (cast_photos + media_links) ----------
        yield progress(
            {
                "stage": "word_id",
                "message": "Detecting words/text overlays...",
                "current": None,
                "total": None,
            }
        )

        to_cast, ts_cast, tu_cast, tf_cast = _detect_text_overlay_cast_photos(
            db,
            person_id_str,
            sources,
            reason_counts=text_overlay_reason_counts,
        )
        to_media, ts_media, tu_media, tf_media = _detect_text_overlay_media_links(
            db,
            person_id_str,
            reason_counts=text_overlay_reason_counts,
        )
        text_overlay_attempted = to_cast + to_media
        text_overlay_succeeded = ts_cast + ts_media
        text_overlay_unknown = tu_cast + tu_media
        text_overlay_failed = tf_cast + tf_media

        yield progress(
            {
                "stage": "word_id",
                "message": (
                    "Text detection done "
                    f"({text_overlay_succeeded} succeeded, {text_overlay_unknown} "
                    f"unknown, {text_overlay_failed} failed)."
                ),
                "current": text_overlay_attempted,
                "total": text_overlay_attempted,
            }
        )

        # ---------- Centering / cropping ----------
        yield progress(
            {
                "stage": "centering_cropping",
                "message": "Centering/cropping thumbnails...",
                "current": None,
                "total": None,
            }
        )

        c_attempted, c_succeeded, c_failed, c_skipped = _recenter_person_gallery_images(
            db,
            person_id_str,
            sources,
            force=True,
        )

        yield progress(
            {
                "stage": "centering_cropping",
                "message": f"Centered {c_succeeded} thumbnails ({c_failed} failed, {c_skipped} manual skipped).",
                "current": c_attempted,
                "total": c_attempted,
            }
        )

        # ---------- Complete ----------
        complete_data = {
            "person_id": person_id_str,
            "run_id": run_id,
            "auto_counts_attempted": auto_counts_attempted,
            "auto_counts_succeeded": auto_counts_succeeded,
            "auto_counts_failed": auto_counts_failed,
            "text_overlay_attempted": text_overlay_attempted,
            "text_overlay_succeeded": text_overlay_succeeded,
            "text_overlay_unknown": text_overlay_unknown,
            "text_overlay_failed": text_overlay_failed,
            "text_overlay_failure_reasons": text_overlay_reason_counts,
            "centering_attempted": c_attempted,
            "centering_succeeded": c_succeeded,
            "centering_failed": c_failed,
            "centering_skipped_manual": c_skipped,
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


@router.patch("/{person_id}/gallery/{link_id}/facebank-seed", response_model=FacebankSeedResponse)
def update_facebank_seed(
    person_id: UUID,
    link_id: UUID,
    payload: FacebankSeedRequest,
    db: SupabaseAdminClient = None,
    _: FacebankSeedAdminUser = None,
) -> FacebankSeedResponse:
    response = (
        db.schema("core")
        .table("media_links")
        .select("id, entity_id, entity_type, kind, facebank_seed")
        .eq("id", str(link_id))
        .limit(1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail="Database error fetching media link")
    if not response.data:
        raise HTTPException(status_code=404, detail="Media link not found")

    row = response.data[0]
    if row.get("entity_type") != "person" or row.get("kind") != "gallery":
        raise HTTPException(status_code=409, detail="Media link is not a person gallery image")
    if str(row.get("entity_id")) != str(person_id):
        raise HTTPException(status_code=409, detail="Media link does not belong to this person")

    try:
        updated = update_media_link_facebank_seed(db, str(link_id), payload.facebank_seed)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Database error updating facebank_seed: {exc}") from exc

    return FacebankSeedResponse(
        link_id=str(updated.get("id") or link_id),
        person_id=str(row.get("entity_id")),
        facebank_seed=bool(updated.get("facebank_seed")),
    )
