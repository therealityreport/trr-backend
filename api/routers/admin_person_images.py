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
from collections.abc import AsyncGenerator, Callable
from typing import Any, Literal
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
    text_overlay_failed: int = 0
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


def _pick_autocount_url(row: dict[str, Any]) -> str | None:
    source = str(row.get("source") or "").lower()
    image_url = row.get("image_url") or row.get("url")
    thumb_url = row.get("thumb_url")
    hosted_url = row.get("hosted_url")

    if source == "tmdb":
        if _is_http_url(image_url):
            return str(image_url)
        if _is_http_url(row.get("url")):
            return str(row.get("url"))

    if source in ("fandom", "fandom-gallery"):
        if _is_wikia_static_url(thumb_url):
            return str(thumb_url)
        if _is_http_url(image_url):
            return str(image_url)
        if _is_http_url(row.get("url")):
            return str(row.get("url"))

    if _is_http_url(image_url):
        return str(image_url)
    if _is_http_url(row.get("url")):
        return str(row.get("url"))
    if _is_http_url(thumb_url):
        return str(thumb_url)
    if _is_http_url(hosted_url):
        return str(hosted_url)
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
            .select("id, hosted_url, hosted_content_type, url, image_url, thumb_url, people_names, source")
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
        screenalytics_available = True
        for row in rows:
            if row.get("people_names"):
                continue
            tag_row = tag_rows.get(str(row["id"]))
            if has_manual_tags(tag_row):
                continue
            if tag_row and tag_row.get("people_count") is not None:
                continue
            if not screenalytics_available:
                break
            image_url = _pick_autocount_url(row)
            if not image_url:
                continue
            auto_counts_attempted += 1
            try:
                result = count_people(image_url)
                upsert_cast_photo_tags(
                    db,
                    cast_photo_id=str(row["id"]),
                    people_names=None,
                    people_ids=None,
                    people_count=result.people_count,
                    people_count_source="auto",
                    detector=result.detector,
                    updated_by_firebase_uid="system:auto",
                )
                auto_counts_succeeded += 1
            except ScreenalyticsClientError as exc:
                auto_counts_failed += 1
                logger.warning("Auto-count failed for %s: %s", row.get("id"), exc)
                message = str(exc).lower()
                if "404" in message or "not found" in message:
                    screenalytics_available = False
    except Exception as exc:
        logger.exception("Auto-count setup failed for %s: %s", person_id, exc)

    return auto_counts_attempted, auto_counts_succeeded, auto_counts_failed


def _chunked(values: list[str], size: int = 100) -> list[list[str]]:
    return [values[i : i + size] for i in range(0, len(values), size)]


def _enrich_cast_photos_with_episode_metadata(
    db: SupabaseAdminClient,
    photos: list[dict[str, Any]],
) -> None:
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
        return

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
            continue
        for row in response.data or []:
            imdb_episode_id = row.get("imdb_episode_id")
            if imdb_episode_id:
                episodes_by_imdb[str(imdb_episode_id)] = row

    if not episodes_by_imdb:
        return

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


def _apply_show_context_to_photos(
    db: SupabaseAdminClient,
    photos: list[dict[str, Any]],
    *,
    show_id: UUID | None,
    show_name: str | None,
) -> None:
    if not photos:
        return
    if show_id is None and not (isinstance(show_name, str) and show_name.strip()):
        return

    show_id_str = str(show_id) if show_id is not None else None
    show_name_val = show_name.strip() if isinstance(show_name, str) and show_name.strip() else None

    if show_id_str and not show_name_val:
        resp = db.schema("core").table("shows").select("id,name").eq("id", show_id_str).limit(1).execute()
        if not (hasattr(resp, "error") and resp.error) and resp.data:
            show_name_val = str(resp.data[0].get("name") or "").strip() or None

    for row in photos:
        metadata = dict(row.get("metadata") or {})
        if show_id_str:
            metadata.setdefault("show_id", show_id_str)
        if show_name_val:
            metadata.setdefault("show_name", show_name_val)
        row["metadata"] = metadata


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
        html, final_url = fetch_fandom_person_html(url)
        if not html:
            continue
        cast_fandom, _photos = parse_fandom_person_html(html, source_url=final_url)
        if not isinstance(cast_fandom, dict) or not cast_fandom:
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
) -> tuple[int, int, int]:
    """
    Best-effort "word id" detection for cast_photos rows.

    Returns (attempted, succeeded, failed).
    """
    attempted = 0
    succeeded = 0
    failed = 0

    candidate_sources = [s for s in sources if s in ALL_SOURCES]
    if not candidate_sources:
        return attempted, succeeded, failed
    if photo_ids is not None and not photo_ids:
        photo_ids = None

    try:
        from trr_backend.vision.text_overlay import (
            TextOverlayDetectionError,
            detect_and_update_cast_photo_text_overlay,
            is_text_overlay_detection_configured,
        )

        if not is_text_overlay_detection_configured():
            return attempted, succeeded, failed

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
            return attempted, succeeded, failed

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
                detect_and_update_cast_photo_text_overlay(db, photo_id, force=False)
                succeeded += 1
            except TextOverlayDetectionError as exc:
                failed += 1
                logger.warning("Word detection failed photo_id=%s error=%s", photo_id, exc)
            except Exception as exc:  # noqa: BLE001
                failed += 1
                logger.warning("Word detection failed photo_id=%s error=%s", photo_id, exc)

            if progress_cb:
                progress_cb(idx, total)
    except Exception as exc:
        logger.exception("Word detection setup failed for %s: %s", person_id, exc)

    return attempted, succeeded, failed


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

    # 1.5 Refresh person profiles (best-effort)
    try:
        _refresh_tmdb_profile(db, person_id_str, tmdb_person_id=tmdb_person_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("TMDb profile refresh failed for %s: %s", person_id, exc)
        errors.append(f"TMDb profile: {exc}")
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
    else:
        try:
            _enrich_cast_photos_with_episode_metadata(db, photos)
        except Exception as exc:
            logger.warning("Episode metadata enrichment failed for %s: %s", person_id, exc)
        try:
            _apply_show_context_to_photos(
                db,
                photos,
                show_id=request.show_id,
                show_name=request.show_name,
            )
        except Exception as exc:
            logger.warning("Show context tagging failed for %s: %s", person_id, exc)

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
    auto_counts_attempted, auto_counts_succeeded, auto_counts_failed = _auto_count_cast_photos(
        db,
        person_id_str,
        sources,
        photo_ids=upserted_photo_ids,
    )

    # 4.6 Word ID / text overlay detection (best-effort)
    text_overlay_attempted, text_overlay_succeeded, text_overlay_failed = _detect_text_overlay_cast_photos(
        db,
        person_id_str,
        sources,
        photo_ids=upserted_photo_ids,
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
        text_overlay_failed=text_overlay_failed,
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

    async def event_generator() -> AsyncGenerator[str, None]:
        errors: list[str] = []
        upserted_photo_ids: list[str] = []

        def progress(payload: dict[str, Any]) -> str:
            return f"event: progress\ndata: {json.dumps(payload)}\n\n"

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

        # 1.5 Refresh person profiles (best-effort)
        yield progress({"stage": "tmdb_profile", "message": "Syncing TMDb profile..."})
        try:
            _refresh_tmdb_profile(db, person_id_str, tmdb_person_id=tmdb_person_id)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"TMDb profile: {exc}")

        yield progress({"stage": "fandom_profile", "message": "Syncing Fandom profile..."})
        try:
            _refresh_fandom_profile(db, person_id_str, person_name=person_name)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"Fandom profile: {exc}")

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

        try:
            _enrich_cast_photos_with_episode_metadata(db, photos)
        except Exception as exc:
            errors.append(str(exc))
        try:
            _apply_show_context_to_photos(
                db,
                photos,
                show_id=request.show_id,
                show_name=request.show_name,
            )
        except Exception as exc:
            errors.append(str(exc))

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

        # 5.5 Auto-count people (best-effort; skips manual tags)
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

            if is_screenalytics_configured():
                candidate_sources = [s for s in sources if s in ALL_SOURCES]
                query = (
                    db.schema("core")
                    .table("cast_photos")
                    .select("id, hosted_url, hosted_content_type, url, image_url, thumb_url, people_names, source")
                    .eq("person_id", person_id_str)
                    .in_("source", candidate_sources)
                )
                if upserted_photo_ids:
                    query = query.in_("id", upserted_photo_ids)
                response = query.execute()
                rows = response.data or []
                tag_rows = get_tags_by_photo_ids(db, [str(row["id"]) for row in rows if row.get("id")])

                to_process = []
                for row in rows:
                    if row.get("people_names"):
                        continue
                    tag_row = tag_rows.get(str(row["id"]))
                    if has_manual_tags(tag_row):
                        continue
                    if tag_row and tag_row.get("people_count") is not None:
                        continue
                    url = _pick_autocount_url(row)
                    if not url:
                        continue
                    to_process.append((str(row["id"]), url))

                yield progress(
                    {
                        "stage": "auto_count",
                        "message": "Auto-counting people in images...",
                        "current": 0,
                        "total": len(to_process),
                    }
                )
                for idx, (photo_id, url) in enumerate(to_process, start=1):
                    auto_counts_attempted += 1
                    try:
                        result = count_people(url)
                        upsert_cast_photo_tags(
                            db,
                            cast_photo_id=photo_id,
                            people_names=None,
                            people_ids=None,
                            people_count=result.people_count,
                            people_count_source="auto",
                            detector=result.detector,
                            updated_by_firebase_uid="system:auto",
                        )
                        auto_counts_succeeded += 1
                    except ScreenalyticsClientError as exc:
                        auto_counts_failed += 1
                        errors.append(f"Auto-count {photo_id}: {exc}")
                    if idx <= 20 or idx % 5 == 0 or idx == len(to_process):
                        yield progress(
                            {
                                "stage": "auto_count",
                                "message": "Auto-counting people in images...",
                                "current": idx,
                                "total": len(to_process),
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
        text_overlay_failed = 0
        try:
            from trr_backend.vision.text_overlay import (
                TextOverlayDetectionError,
                detect_and_update_cast_photo_text_overlay,
                is_text_overlay_detection_configured,
            )

            if is_text_overlay_detection_configured():
                query = (
                    db.schema("core")
                    .table("cast_photos")
                    .select("id, metadata, source")
                    .eq("person_id", person_id_str)
                    .in_("source", [s for s in sources if s in ALL_SOURCES])
                )
                if upserted_photo_ids:
                    query = query.in_("id", upserted_photo_ids)
                response = query.execute()
                rows = response.data or []
                to_process = []
                for row in rows:
                    meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
                    if "has_text_overlay" in (meta or {}):
                        continue
                    rid = row.get("id")
                    if rid:
                        to_process.append(str(rid))

                yield progress(
                    {
                        "stage": "word_id",
                        "message": "Detecting words/text overlays...",
                        "current": 0,
                        "total": len(to_process),
                    }
                )
                for idx, photo_id in enumerate(to_process, start=1):
                    text_overlay_attempted += 1
                    try:
                        detect_and_update_cast_photo_text_overlay(db, photo_id, force=False)
                        text_overlay_succeeded += 1
                    except TextOverlayDetectionError as exc:
                        text_overlay_failed += 1
                        errors.append(f"Word ID {photo_id}: {exc}")
                    except Exception as exc:  # noqa: BLE001
                        text_overlay_failed += 1
                        errors.append(f"Word ID {photo_id}: {exc}")
                    if idx <= 20 or idx % 5 == 0 or idx == len(to_process):
                        yield progress(
                            {
                                "stage": "word_id",
                                "message": "Detecting words/text overlays...",
                                "current": idx,
                                "total": len(to_process),
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

        # 6. Complete
        complete_data = {
            "person_id": person_id_str,
            "photos_fetched": len(photos),
            "photos_upserted": photos_upserted,
            "photos_mirrored": photos_mirrored,
            "photos_failed": photos_failed,
            "photos_pruned": photos_pruned,
            "auto_counts_attempted": auto_counts_attempted,
            "auto_counts_succeeded": auto_counts_succeeded,
            "auto_counts_failed": auto_counts_failed,
            "text_overlay_attempted": text_overlay_attempted,
            "text_overlay_succeeded": text_overlay_succeeded,
            "text_overlay_failed": text_overlay_failed,
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
