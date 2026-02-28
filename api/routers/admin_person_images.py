"""
Admin endpoints for person image management.

Provides endpoints to:
1. Refresh images from sources (IMDb, TMDb, Fandom)
2. Mirror images to S3
3. Stream progress via SSE
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import unicodedata
from collections.abc import AsyncGenerator, Callable
from datetime import UTC, datetime
from threading import Lock
from typing import Any, Literal
from urllib.parse import unquote, urlparse
from uuid import UUID

from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from api.auth import AdminUser, FacebankSeedAdminUser
from api.deps import SupabaseAdminClient
from trr_backend.media.face_crops import generate_and_upload_face_crops
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
IMDB_STRICT_ALLOWED_TYPES = {"event", "still_frame"}
IMDB_REFRESH_DIAGNOSTIC_FIELDS = (
    "imdb_pages_scanned",
    "imdb_candidates_seen",
    "imdb_kept",
    "imdb_filtered_type",
    "imdb_filtered_people",
    "imdb_filtered_episode",
    "imdb_filtered_other",
)
IMDB_VIEWER_ID_RE = re.compile(r"/mediaviewer/(rm\d+)/", re.IGNORECASE)


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
    skip_auto_count: bool = Field(
        default=False,
        description="Skip auto people counting stage",
    )
    skip_word_detection: bool = Field(
        default=False,
        description="Skip word/text overlay detection stage",
    )
    skip_centering: bool = Field(
        default=False,
        description="Skip face centering/cropping stage",
    )
    skip_resize: bool = Field(
        default=False,
        description="Skip resize/variant generation stage",
    )
    show_id: UUID | None = Field(
        default=None,
        description="Optional show context to tag all fetched photos with show_id/show_name for filtering.",
    )
    show_name: str | None = Field(
        default=None,
        description="Optional show name to tag all fetched photos with show_id/show_name for filtering.",
    )
    enforce_show_source_policy: bool = Field(
        default=True,
        description=(
            "Apply show-based source restrictions (e.g., disabling Fandom for non-Real Housewives context). "
            "Set false to always use requested sources."
        ),
    )


class RefreshImagesResponse(BaseModel):
    """Response after refreshing person images."""

    person_id: str
    person_name: str | None
    imdb_person_id: str | None
    tmdb_person_id: int | None
    tmdb_profile_status: Literal["ok", "skipped", "failed"] | None = None
    tmdb_profile_error_code: str | None = None
    tmdb_profile_error_detail: str | None = None
    sources_used: list[str]
    photos_fetched: int
    photos_upserted: int
    photos_mirrored: int
    photos_failed: int
    cast_photos_mirrored: int = 0
    cast_photos_failed: int = 0
    media_assets_mirrored: int = 0
    media_assets_failed: int = 0
    photos_pruned: int
    imdb_pages_scanned: int = 0
    imdb_candidates_seen: int = 0
    imdb_kept: int = 0
    imdb_filtered_type: int = 0
    imdb_filtered_people: int = 0
    imdb_filtered_episode: int = 0
    imdb_filtered_other: int = 0
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
    resize_attempted: int = 0
    resize_succeeded: int = 0
    resize_failed: int = 0
    resize_crop_attempted: int = 0
    resize_crop_succeeded: int = 0
    resize_crop_failed: int = 0
    errors: list[str] = Field(default_factory=list)


class ReprocessImagesRequest(BaseModel):
    """Request to reprocess existing gallery assets."""

    run_count: bool = Field(default=True, description="Run people auto-count stage.")
    run_id_text: bool = Field(default=True, description="Run text overlay detection stage.")
    run_crop: bool = Field(default=True, description="Run centering/cropping stage.")
    run_resize: bool = Field(default=True, description="Run resize/variant generation stage.")
    sources: list[SourceType] | None = Field(
        default=None,
        description="Optional source filter for cast-photo stages.",
    )


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
    response = db.schema("core").table("shows").select("name").eq("id", str(show_id)).limit(1).execute()
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


def _resolve_refresh_sources(
    db: SupabaseAdminClient,
    request: RefreshImagesRequest,
) -> tuple[list[SourceType], bool]:
    requested_sources = list(request.sources or ALL_SOURCES)
    if not request.enforce_show_source_policy:
        return requested_sources, False
    return _apply_show_source_policy(db, request.show_id, requested_sources)


def _extract_imdb_id(external_ids: dict | None) -> str | None:
    """Extract IMDb person ID from external_ids."""
    if not external_ids:
        return None
    return external_ids.get("imdb")


def _extract_imdb_id_from_external_ids(external_ids: Any) -> str | None:
    if not isinstance(external_ids, dict):
        return None
    for key in ("imdb", "imdb_id", "imdb_episode_id", "imdb_series_id"):
        value = external_ids.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _load_show_imdb_title_ids(db: SupabaseAdminClient, show_id: UUID | None) -> set[str]:
    if show_id is None:
        return set()
    show_id_str = str(show_id)
    imdb_ids: set[str] = set()
    try:
        show_resp = (
            db.schema("core")
            .table("shows")
            .select("imdb_id")
            .eq("id", show_id_str)
            .limit(1)
            .execute()
        )
        show_rows = show_resp.data or []
        if show_rows and isinstance(show_rows[0], dict):
            show_imdb_id = show_rows[0].get("imdb_id")
            if isinstance(show_imdb_id, str) and show_imdb_id.strip():
                imdb_ids.add(show_imdb_id.strip().lower())
    except Exception:  # noqa: BLE001
        pass

    try:
        episode_resp = (
            db.schema("core")
            .table("episodes")
            .select("external_ids")
            .eq("show_id", show_id_str)
            .limit(5000)
            .execute()
        )
        episode_rows = episode_resp.data or []
        for row in episode_rows:
            external_ids = row.get("external_ids") if isinstance(row, dict) else None
            episode_imdb_id = _extract_imdb_id_from_external_ids(external_ids)
            if episode_imdb_id:
                imdb_ids.add(episode_imdb_id.lower())
    except Exception:  # noqa: BLE001
        pass
    return imdb_ids


def _build_imdb_focus_keywords(show_name: str | None) -> list[str]:
    normalized = str(show_name or "").strip().lower()
    if not normalized:
        return []
    if "traitors" in normalized:
        return ["the traitors", "traitors"]
    if "watch what happens live" in normalized or "wwhl" in normalized:
        return ["watch what happens live", "watch what happens live with andy cohen", "wwhl"]
    return []


def _resolve_imdb_focus_filters(
    db: SupabaseAdminClient,
    show_id: UUID | None,
    show_name: str | None,
) -> tuple[set[str], list[str], bool]:
    resolved_show_name = show_name
    if not (isinstance(resolved_show_name, str) and resolved_show_name.strip()) and show_id is not None:
        resolved_show_name = _get_show_name(db, show_id)
    keywords = _build_imdb_focus_keywords(resolved_show_name)
    if not keywords:
        return set(), [], False
    title_ids = _load_show_imdb_title_ids(db, show_id)
    return title_ids, keywords, True


def _empty_imdb_refresh_diagnostics() -> dict[str, int]:
    return dict.fromkeys(IMDB_REFRESH_DIAGNOSTIC_FIELDS, 0)


def _is_traitors_show_name(show_name: str | None) -> bool:
    normalized = str(show_name or "").strip().lower()
    return bool(normalized) and "traitors" in normalized


def _load_show_episode_imdb_ids(db: SupabaseAdminClient, show_id: UUID | None) -> set[str]:
    if show_id is None:
        return set()
    show_id_str = str(show_id)
    episode_ids: set[str] = set()
    try:
        response = (
            db.schema("core")
            .table("episodes")
            .select("external_ids")
            .eq("show_id", show_id_str)
            .limit(5000)
            .execute()
        )
    except Exception:  # noqa: BLE001
        return set()
    rows = response.data if isinstance(getattr(response, "data", None), list) else []
    for row in rows:
        external_ids = row.get("external_ids") if isinstance(row, dict) else None
        episode_imdb_id = _extract_imdb_id_from_external_ids(external_ids)
        if episode_imdb_id:
            episode_ids.add(episode_imdb_id.lower())
    return episode_ids


def _load_show_cast_identity_sets(
    db: SupabaseAdminClient,
    show_id: UUID | None,
) -> tuple[set[str], set[str]]:
    if show_id is None:
        return set(), set()
    show_id_str = str(show_id)
    person_ids: list[str] = []
    try:
        response = (
            db.schema("core")
            .table("show_cast")
            .select("person_id")
            .eq("show_id", show_id_str)
            .limit(5000)
            .execute()
        )
    except Exception:  # noqa: BLE001
        return set(), set()

    rows = response.data if isinstance(getattr(response, "data", None), list) else []
    for row in rows:
        person_id = str(row.get("person_id") or "").strip() if isinstance(row, dict) else ""
        if person_id:
            person_ids.append(person_id)
    if not person_ids:
        return set(), set()

    cast_imdb_ids: set[str] = set()
    cast_names: set[str] = set()
    for start in range(0, len(person_ids), 500):
        chunk = person_ids[start : start + 500]
        try:
            people_response = (
                db.schema("core")
                .table("people")
                .select("id,full_name,external_ids")
                .in_("id", chunk)
                .execute()
            )
        except Exception:  # noqa: BLE001
            continue
        people_rows = people_response.data if isinstance(getattr(people_response, "data", None), list) else []
        for person_row in people_rows:
            if not isinstance(person_row, dict):
                continue
            full_name = str(person_row.get("full_name") or "").strip()
            if full_name:
                cast_names.add(full_name)
            imdb_id = _extract_imdb_id_from_external_ids(person_row.get("external_ids"))
            if imdb_id:
                cast_imdb_ids.add(imdb_id.lower())
    return cast_imdb_ids, cast_names


def _resolve_imdb_traitors_strict_context(
    db: SupabaseAdminClient,
    *,
    show_id: UUID | None,
    show_name: str | None,
    target_person_imdb_id: str | None,
    target_person_name: str | None,
) -> dict[str, Any]:
    resolved_show_id = show_id
    resolved_show_name = show_name.strip() if isinstance(show_name, str) and show_name.strip() else None
    if resolved_show_id is not None and not resolved_show_name:
        resolved_show_name = _get_show_name(db, resolved_show_id)
    if resolved_show_id is None and resolved_show_name:
        try:
            _, by_alias, _ = _build_show_lookup_maps(db)
            show_row = _find_show_row_by_alias(by_alias, resolved_show_name)
            show_id_val = str(show_row.get("id") or "").strip() if isinstance(show_row, dict) else ""
            if show_id_val:
                resolved_show_id = UUID(show_id_val)
            if isinstance(show_row, dict):
                resolved_show_name = str(show_row.get("name") or "").strip() or resolved_show_name
        except Exception:  # noqa: BLE001
            pass

    strict_mode_enabled = _is_traitors_show_name(resolved_show_name)
    cast_imdb_ids: set[str] = set()
    cast_names: set[str] = set()
    episode_imdb_ids: set[str] = set()
    if strict_mode_enabled:
        cast_imdb_ids, cast_names = _load_show_cast_identity_sets(db, resolved_show_id)
        episode_imdb_ids = _load_show_episode_imdb_ids(db, resolved_show_id)

    target_imdb = str(target_person_imdb_id or "").strip().lower() or None
    target_name = str(target_person_name or "").strip() or None
    if target_imdb:
        cast_imdb_ids.add(target_imdb)
    if target_name:
        cast_names.add(target_name)

    return {
        "strict_mode_enabled": strict_mode_enabled,
        "strict_types": set(IMDB_STRICT_ALLOWED_TYPES) if strict_mode_enabled else set(),
        "target_person_imdb_id": target_imdb,
        "target_person_name": target_name,
        "allowed_cast_imdb_ids": cast_imdb_ids,
        "allowed_cast_names": cast_names,
        "allowed_episode_imdb_ids": episode_imdb_ids,
        "resolved_show_id": str(resolved_show_id) if resolved_show_id else None,
        "resolved_show_name": resolved_show_name,
    }


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


def _count_mirrored_cast_photos(db: SupabaseAdminClient, person_id: str, source: str) -> int:
    try:
        response = (
            db.schema("core")
            .table("cast_photos")
            .select("id", count="exact")
            .eq("person_id", person_id)
            .eq("source", source)
            .not_.is_("hosted_url", "null")
            .execute()
        )
        if hasattr(response, "count") and response.count is not None:
            return int(response.count)
        data = response.data or []
        return len(data) if isinstance(data, list) else 0
    except Exception:  # noqa: BLE001
        return 0


def _get_known_source_total(
    source: SourceType,
    imdb_person_id: str | None,
    tmdb_person_id: int | None,
) -> int | None:
    if source == "imdb" and imdb_person_id:
        try:
            from trr_backend.integrations.imdb.person_gallery import (
                extract_imdb_person_mediaindex_total,
                fetch_imdb_person_mediaindex_html,
                parse_imdb_person_mediaindex_images,
            )

            html = fetch_imdb_person_mediaindex_html(imdb_person_id, session=None)
            total = extract_imdb_person_mediaindex_total(html)
            if isinstance(total, int) and total >= 0:
                return total
            images = parse_imdb_person_mediaindex_images(html, imdb_person_id)
            return len(images) if isinstance(images, list) else None
        except Exception:  # noqa: BLE001
            return None
    if source == "tmdb" and tmdb_person_id:
        try:
            from trr_backend.integrations.tmdb.client import fetch_person_images

            payload = fetch_person_images(int(tmdb_person_id), session=None)
            profiles = payload.get("profiles") if isinstance(payload, dict) else None
            return len(profiles) if isinstance(profiles, list) else None
        except Exception:  # noqa: BLE001
            return None
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


def _normalize_face_coord(value: float) -> float:
    return round(max(0.0, min(1.0, value)), 4)


def _build_face_boxes(result: Any) -> list[dict[str, Any]]:
    detections = getattr(result, "detections", None) or []
    boxes: list[dict[str, Any]] = []
    index = 1
    for det in detections:
        kind = str(getattr(det, "kind", "face")).lower()
        if kind != "face":
            continue
        try:
            x1 = float(det.x1)
            y1 = float(det.y1)
            x2 = float(det.x2)
            y2 = float(det.y2)
        except (AttributeError, TypeError, ValueError):
            continue

        x1 = _normalize_face_coord(x1)
        y1 = _normalize_face_coord(y1)
        x2 = _normalize_face_coord(x2)
        y2 = _normalize_face_coord(y2)
        width = round(max(0.0, x2 - x1), 4)
        height = round(max(0.0, y2 - y1), 4)
        if width <= 0 or height <= 0:
            continue

        confidence_raw = getattr(det, "confidence", None)
        confidence = (
            round(max(0.0, min(1.0, float(confidence_raw))), 4) if isinstance(confidence_raw, (int, float)) else None
        )
        person_id_raw = getattr(det, "person_id", None)
        person_name_raw = getattr(det, "person_name", None)
        label_raw = getattr(det, "label", None)
        match_similarity_raw = getattr(det, "match_similarity", None)
        match_status_raw = getattr(det, "match_status", None)
        square_crop_bbox_raw = getattr(det, "square_crop_bbox", None)
        person_id = str(person_id_raw).strip() if isinstance(person_id_raw, str) and person_id_raw.strip() else None
        person_name = (
            str(person_name_raw).strip() if isinstance(person_name_raw, str) and person_name_raw.strip() else None
        )
        label = str(label_raw).strip() if isinstance(label_raw, str) and label_raw.strip() else None
        match_similarity = (
            round(max(0.0, min(1.0, float(match_similarity_raw))), 4)
            if isinstance(match_similarity_raw, (int, float))
            else None
        )
        match_status = (
            str(match_status_raw).strip().lower()
            if isinstance(match_status_raw, str) and match_status_raw.strip()
            else None
        )
        square_crop_bbox: list[float] | None = None
        if isinstance(square_crop_bbox_raw, list) and len(square_crop_bbox_raw) >= 4:
            try:
                sx1 = _normalize_face_coord(float(square_crop_bbox_raw[0]))
                sy1 = _normalize_face_coord(float(square_crop_bbox_raw[1]))
                sx2 = _normalize_face_coord(float(square_crop_bbox_raw[2]))
                sy2 = _normalize_face_coord(float(square_crop_bbox_raw[3]))
                if sx2 > sx1 and sy2 > sy1:
                    square_crop_bbox = [sx1, sy1, sx2, sy2]
            except (TypeError, ValueError):
                square_crop_bbox = None

        boxes.append(
            {
                "index": index,
                "kind": "face",
                "x": x1,
                "y": y1,
                "width": width,
                "height": height,
                "confidence": confidence,
                **({"person_id": person_id} if person_id else {}),
                **({"person_name": person_name} if person_name else {}),
                **({"label": label} if label else {}),
                **({"match_similarity": match_similarity} if match_similarity is not None else {}),
                **({"match_status": match_status} if match_status else {}),
                **({"square_crop_bbox": square_crop_bbox} if square_crop_bbox else {}),
            }
        )
        index += 1
    return boxes


def _auto_people_from_face_boxes(face_boxes: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    people_ids: list[str] = []
    people_names: list[str] = []
    seen_ids: set[str] = set()
    seen_names: set[str] = set()
    for box in face_boxes:
        person_id = box.get("person_id")
        person_name = box.get("person_name")
        if isinstance(person_id, str) and person_id.strip():
            normalized = person_id.strip()
            if normalized not in seen_ids:
                seen_ids.add(normalized)
                people_ids.append(normalized)
        if isinstance(person_name, str) and person_name.strip():
            normalized_name = person_name.strip()
            key = normalized_name.lower()
            if key not in seen_names:
                seen_names.add(key)
                people_names.append(normalized_name)
    return people_ids, people_names


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
        .select(
            "id, source, source_url, hosted_url, hosted_sha256, hosted_key, hosted_bucket, "
            "hosted_content_type, hosted_bytes, hosted_etag, width, height, metadata"
        )
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
                "hosted_sha256": asset.get("hosted_sha256"),
                "hosted_key": asset.get("hosted_key"),
                "hosted_bucket": asset.get("hosted_bucket"),
                "hosted_content_type": asset.get("hosted_content_type"),
                "hosted_bytes": asset.get("hosted_bytes"),
                "hosted_etag": asset.get("hosted_etag"),
                "width": asset.get("width"),
                "height": asset.get("height"),
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
    progress_cb: Callable[[int, int], None] | None = None,
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
    total_rows = len(rows)
    for idx, row in enumerate(rows, start=1):
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
        if progress_cb:
            progress_cb(idx, total_rows)
    return mirrored, failed


def _mirror_person_media_assets(
    db: SupabaseAdminClient,
    person_id: str,
    *,
    force: bool = False,
    progress_cb: Callable[[int, int], None] | None = None,
) -> tuple[int, int]:
    from trr_backend.media.s3_mirror import mirror_media_asset_row
    from trr_backend.repositories.media_assets import (
        update_asset_with_mirror_result,
        update_ingest_status,
    )

    rows = _fetch_person_media_link_rows(db, person_id)
    assets_by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        asset_id = str(row.get("media_asset_id") or "")
        if not asset_id or asset_id in assets_by_id:
            continue
        assets_by_id[asset_id] = row

    unique_assets = list(assets_by_id.values())
    if not unique_assets:
        return 0, 0

    mirrored = 0
    failed = 0
    total_rows = len(unique_assets)
    for idx, row in enumerate(unique_assets, start=1):
        asset_id = str(row.get("media_asset_id") or "")
        if not asset_id:
            failed += 1
            if progress_cb:
                progress_cb(idx, total_rows)
            continue
        try:
            update_ingest_status(db, asset_id, "in_progress")
            patch = mirror_media_asset_row(row, force=force)
            if patch:
                if set(patch.keys()) == {"hosted_url"}:
                    db.schema("core").table("media_assets").update({"hosted_url": patch["hosted_url"]}).eq(
                        "id", asset_id
                    ).execute()
                    update_ingest_status(
                        db,
                        asset_id,
                        "hosted",
                        completed_at=datetime.now(UTC).isoformat(),
                    )
                else:
                    completed_at = str(patch.get("hosted_at") or datetime.now(UTC).isoformat())
                    update_asset_with_mirror_result(
                        db,
                        asset_id=asset_id,
                        sha256=str(patch.get("sha256") or patch.get("hosted_sha256") or ""),
                        hosted_bucket=str(patch.get("hosted_bucket") or ""),
                        hosted_key=str(patch.get("hosted_key") or ""),
                        hosted_url=str(patch.get("hosted_url") or ""),
                        hosted_bytes=int(patch.get("hosted_bytes") or 0),
                        hosted_content_type=(
                            str(patch.get("hosted_content_type"))
                            if patch.get("hosted_content_type") is not None
                            else None
                        ),
                        hosted_etag=(str(patch.get("hosted_etag")) if patch.get("hosted_etag") is not None else None),
                        width=int(patch.get("width")) if patch.get("width") is not None else None,
                        height=int(patch.get("height")) if patch.get("height") is not None else None,
                        completed_at=completed_at,
                        metadata=patch.get("metadata") if isinstance(patch.get("metadata"), dict) else None,
                    )
                mirrored += 1
            else:
                update_ingest_status(
                    db,
                    asset_id,
                    "hosted",
                    completed_at=datetime.now(UTC).isoformat(),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Mirror failed for media asset %s: %s", asset_id, exc)
            failed += 1
            try:
                update_ingest_status(
                    db,
                    asset_id,
                    "failed",
                    error=str(exc),
                    failed_at=datetime.now(UTC).isoformat(),
                )
            except Exception:  # noqa: BLE001
                pass
        if progress_cb:
            progress_cb(idx, total_rows)

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
            selected_image_url: str | None = None
            last_error: ScreenalyticsClientError | None = None
            for image_url in item["image_urls"]:
                try:
                    result = count_people(image_url)
                    selected_image_url = image_url
                    break
                except ScreenalyticsClientError as exc:
                    last_error = exc
            try:
                if result is None:
                    raise last_error or ScreenalyticsClientError("Unable to auto-count image")
                face_boxes = _build_face_boxes(result)
                auto_people_ids, auto_people_names = _auto_people_from_face_boxes(face_boxes)
                existing_people_names = tag_row.get("people_names") if tag_row else None
                existing_people_ids = tag_row.get("people_ids") if tag_row else None
                upsert_cast_photo_tags(
                    db,
                    cast_photo_id=item["photo_id"],
                    people_names=existing_people_names if existing_people_names else (auto_people_names or None),
                    people_ids=existing_people_ids if existing_people_ids else (auto_people_ids or None),
                    people_count=result.people_count,
                    people_count_source="auto",
                    detector=result.detector,
                    updated_by_firebase_uid="system:auto",
                )
                existing_meta = dict(row.get("metadata") or {})
                metadata_changed = False
                face_crops: list[dict[str, Any]] = []
                if selected_image_url and face_boxes:
                    face_crops = generate_and_upload_face_crops(
                        entity_kind="cast_photo",
                        entity_id=item["photo_id"],
                        image_url=selected_image_url,
                        face_boxes=face_boxes,
                        size=256,
                    )
                if face_boxes:
                    if existing_meta.get("face_boxes") != face_boxes:
                        existing_meta["face_boxes"] = face_boxes
                        metadata_changed = True
                elif "face_boxes" in existing_meta:
                    existing_meta.pop("face_boxes", None)
                    metadata_changed = True
                if face_crops:
                    if existing_meta.get("face_crops") != face_crops:
                        existing_meta["face_crops"] = face_crops
                        metadata_changed = True
                elif "face_crops" in existing_meta:
                    existing_meta.pop("face_crops", None)
                    metadata_changed = True
                crop_payload = _apply_auto_crop_payload(result)
                if crop_payload is not None:
                    existing_crop = existing_meta.get("thumbnail_crop")
                    if not (isinstance(existing_crop, dict) and existing_crop.get("mode") == "manual"):
                        existing_meta["thumbnail_crop"] = crop_payload
                        metadata_changed = True
                if metadata_changed:
                    try:
                        db.schema("core").table("cast_photos").update({"metadata": existing_meta}).eq(
                            "id", str(row["id"])
                        ).execute()
                    except Exception as crop_exc:
                        logger.warning(
                            "Failed to store auto-count metadata for %s: %s",
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
            selected_image_url: str | None = None
            last_error: ScreenalyticsClientError | None = None
            for image_url in item["urls"]:
                try:
                    result = count_people(image_url)
                    selected_image_url = image_url
                    break
                except ScreenalyticsClientError as exc:
                    last_error = exc
            try:
                if result is None:
                    raise last_error or ScreenalyticsClientError("Unable to auto-count image")
                face_boxes = _build_face_boxes(result)
                auto_people_ids, auto_people_names = _auto_people_from_face_boxes(face_boxes)
                context["people_count"] = result.people_count
                context["people_count_source"] = "auto"
                context["people_count_detector"] = result.detector
                context["face_boxes"] = face_boxes
                face_crops: list[dict[str, Any]] = []
                if selected_image_url and face_boxes:
                    media_asset_id = str(row.get("media_asset_id") or row.get("id") or "").strip()
                    face_crops = generate_and_upload_face_crops(
                        entity_kind="media_asset",
                        entity_id=media_asset_id,
                        image_url=selected_image_url,
                        face_boxes=face_boxes,
                        size=256,
                    )
                context["face_crops"] = face_crops
                if not (
                    isinstance(context.get("people_ids"), list) and context.get("people_ids")
                ) and auto_people_ids:
                    context["people_ids"] = auto_people_ids
                if not (
                    isinstance(context.get("people_names"), list) and context.get("people_names")
                ) and auto_people_names:
                    context["people_names"] = auto_people_names
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


def _resize_person_gallery_images(
    db: SupabaseAdminClient,
    person_id: str,
    sources: list[SourceType],
    *,
    force: bool = False,
    progress_cb: Callable[[int, int], None] | None = None,
) -> tuple[int, int, int, int, int, int]:
    resize_attempted = 0
    resize_succeeded = 0
    resize_failed = 0
    resize_crop_attempted = 0
    resize_crop_succeeded = 0
    resize_crop_failed = 0

    candidate_sources = [s for s in sources if s in ALL_SOURCES]
    if not candidate_sources:
        return (
            resize_attempted,
            resize_succeeded,
            resize_failed,
            resize_crop_attempted,
            resize_crop_succeeded,
            resize_crop_failed,
        )

    try:
        from api.routers.admin_image_counts import auto_count_cast_photo, auto_count_media_asset
        from trr_backend.media.image_variants import (
            generate_cast_photo_variants,
            generate_media_asset_variants,
        )

        cast_rows = (
            db.schema("core")
            .table("cast_photos")
            .select("id, source, hosted_url, metadata")
            .eq("person_id", person_id)
            .in_("source", candidate_sources)
            .not_.is_("hosted_url", "null")
            .execute()
            .data
            or []
        )
        media_rows = _fetch_person_media_link_rows(db, person_id)

        def _normalize_crop_payload(value: Any) -> dict[str, Any] | None:
            if not isinstance(value, dict):
                return None
            try:
                x = float(value.get("x"))
                y = float(value.get("y"))
                zoom = float(value.get("zoom"))
            except (TypeError, ValueError):
                return None
            mode_raw = str(value.get("mode") or "auto").strip().lower()
            mode = "manual" if mode_raw == "manual" else "auto"
            payload: dict[str, Any] = {
                "x": max(0.0, min(100.0, x)),
                "y": max(0.0, min(100.0, y)),
                "zoom": max(1.0, min(4.0, zoom)),
                "mode": mode,
            }
            strategy = value.get("strategy")
            if isinstance(strategy, str) and strategy.strip():
                payload["strategy"] = strategy.strip()
            return payload

        def _fallback_crop_payload() -> dict[str, Any]:
            return {
                "x": 50.0,
                "y": 32.0,
                "zoom": 1.0,
                "mode": "auto",
                "strategy": "resize_center_fallback_v1",
            }

        def _select_best_crop(candidates: list[Any]) -> dict[str, Any] | None:
            manual: dict[str, Any] | None = None
            auto: dict[str, Any] | None = None
            for candidate in candidates:
                normalized = _normalize_crop_payload(candidate)
                if not normalized:
                    continue
                if normalized.get("mode") == "manual":
                    manual = normalized
                    break
                if auto is None:
                    auto = normalized
            return manual or auto

        def _load_crop_from_db(origin: str, target_id: str) -> dict[str, Any] | None:
            if origin == "cast_photos":
                cast_resp = (
                    db.schema("core").table("cast_photos").select("id,metadata").eq("id", target_id).limit(1).execute()
                )
                if hasattr(cast_resp, "error") and cast_resp.error:
                    return None
                cast_rows = cast_resp.data or []
                if not cast_rows:
                    return None
                metadata = cast_rows[0].get("metadata") if isinstance(cast_rows[0].get("metadata"), dict) else {}
                return _select_best_crop([metadata.get("thumbnail_crop") if isinstance(metadata, dict) else None])

            link_resp = (
                db.schema("core")
                .table("media_links")
                .select("id,context")
                .eq("media_asset_id", target_id)
                .limit(250)
                .execute()
            )
            if hasattr(link_resp, "error") and link_resp.error:
                return None
            link_crops = [
                row.get("context", {}).get("thumbnail_crop")
                for row in (link_resp.data or [])
                if isinstance(row, dict) and isinstance(row.get("context"), dict)
            ]
            selected = _select_best_crop(link_crops)
            if selected is not None:
                return selected
            asset_resp = (
                db.schema("core").table("media_assets").select("id,metadata").eq("id", target_id).limit(1).execute()
            )
            if hasattr(asset_resp, "error") and asset_resp.error:
                return None
            asset_rows = asset_resp.data or []
            if not asset_rows:
                return None
            metadata = asset_rows[0].get("metadata") if isinstance(asset_rows[0].get("metadata"), dict) else {}
            return _select_best_crop([metadata.get("thumbnail_crop") if isinstance(metadata, dict) else None])

        def _resolve_crop_for_job(origin: str, target_id: str, existing: Any) -> dict[str, Any]:
            existing_crop = _normalize_crop_payload(existing)
            if existing_crop is not None:
                return existing_crop
            try:
                target_uuid = UUID(str(target_id))
                if origin == "cast_photos":
                    auto_count_cast_photo(target_uuid, force=True, db=db, _=None)
                else:
                    auto_count_media_asset(target_uuid, force=True, db=db, _=None)
            except Exception:
                pass
            detected_crop = _load_crop_from_db(origin, target_id)
            if detected_crop is not None:
                return detected_crop
            return _fallback_crop_payload()

        base_jobs: list[dict[str, Any]] = []
        cast_crops_by_photo: dict[str, dict[str, Any] | None] = {}
        for row in cast_rows:
            photo_id = str(row.get("id") or "")
            if not photo_id:
                continue
            base_jobs.append({"origin": "cast_photos", "id": photo_id, "crop": None})
            metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            cast_crops_by_photo[photo_id] = _normalize_crop_payload(
                metadata.get("thumbnail_crop") if isinstance(metadata, dict) else None
            )

        seen_media_assets: set[str] = set()
        media_crops_by_asset: dict[str, dict[str, Any] | None] = {}
        for row in media_rows:
            asset_id = str(row.get("media_asset_id") or "")
            if not asset_id:
                continue
            if row.get("hosted_url"):
                if asset_id not in seen_media_assets:
                    base_jobs.append({"origin": "media_assets", "id": asset_id, "crop": None})
                    seen_media_assets.add(asset_id)
            context = row.get("context") if isinstance(row.get("context"), dict) else {}
            metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            crop = context.get("thumbnail_crop")
            if not isinstance(crop, dict):
                crop = metadata.get("thumbnail_crop") if isinstance(metadata.get("thumbnail_crop"), dict) else None
            normalized_crop = _normalize_crop_payload(crop)
            if normalized_crop:
                previous = media_crops_by_asset.get(asset_id)
                if not previous:
                    media_crops_by_asset[asset_id] = normalized_crop
                else:
                    prev_mode = str(previous.get("mode") or "").lower()
                    next_mode = str(normalized_crop.get("mode") or "").lower()
                    if prev_mode != "manual" and next_mode == "manual":
                        media_crops_by_asset[asset_id] = normalized_crop

        crop_jobs: list[dict[str, Any]] = []
        for job in base_jobs:
            origin = str(job.get("origin") or "")
            target_id = str(job.get("id") or "")
            if not target_id:
                continue
            if origin == "cast_photos":
                crop_jobs.append(
                    {
                        "origin": origin,
                        "id": target_id,
                        "crop": cast_crops_by_photo.get(target_id),
                    }
                )
            else:
                crop_jobs.append(
                    {
                        "origin": origin,
                        "id": target_id,
                        "crop": media_crops_by_asset.get(target_id),
                    }
                )

        total_ops = len(base_jobs) + len(crop_jobs)
        processed_ops = 0

        for job in base_jobs:
            resize_attempted += 1
            try:
                if job["origin"] == "cast_photos":
                    generate_cast_photo_variants(db, photo_id=job["id"], crop=None, force=force)
                else:
                    generate_media_asset_variants(db, asset_id=job["id"], crop=None, force=force)
                resize_succeeded += 1
            except Exception as exc:  # noqa: BLE001
                resize_failed += 1
                logger.warning(
                    "Resize variants failed origin=%s id=%s error=%s",
                    job["origin"],
                    job["id"],
                    exc,
                )
            processed_ops += 1
            if progress_cb:
                progress_cb(processed_ops, total_ops)

        for job in crop_jobs:
            resize_crop_attempted += 1
            crop_payload = _resolve_crop_for_job(
                str(job.get("origin") or ""),
                str(job.get("id") or ""),
                job.get("crop"),
            )
            try:
                if job["origin"] == "cast_photos":
                    generate_cast_photo_variants(
                        db,
                        photo_id=job["id"],
                        crop=crop_payload,
                        force=force,
                    )
                else:
                    generate_media_asset_variants(
                        db,
                        asset_id=job["id"],
                        crop=crop_payload,
                        force=force,
                    )
                resize_crop_succeeded += 1
            except Exception as exc:  # noqa: BLE001
                resize_crop_failed += 1
                logger.warning(
                    "Crop variants failed origin=%s id=%s error=%s",
                    job["origin"],
                    job["id"],
                    exc,
                )
            processed_ops += 1
            if progress_cb:
                progress_cb(processed_ops, total_ops)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Resize variants setup failed for %s: %s", person_id, exc)

    return (
        resize_attempted,
        resize_succeeded,
        resize_failed,
        resize_crop_attempted,
        resize_crop_succeeded,
        resize_crop_failed,
    )


def _chunked(values: list[str], size: int = 100) -> list[list[str]]:
    return [values[i : i + size] for i in range(0, len(values), size)]


_REAL_HOUSEWIVES_SHORT_CODE_BY_LOCATION: dict[str, str] = {
    "orange county": "RHOC",
    "new york city": "RHONY",
    "new jersey": "RHONJ",
    "atlanta": "RHOA",
    "beverly hills": "RHOBH",
    "potomac": "RHOP",
    "dallas": "RHOD",
    "miami": "RHOM",
    "salt lake city": "RHOSLC",
    "washington d.c.": "RHODC",
    "washington dc": "RHODC",
    "dubai": "RHODubai",
}


def _to_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if raw.isdigit():
            return int(raw)
    return None


def _derive_real_housewives_short_code(show_name: str | None) -> str | None:
    if not isinstance(show_name, str):
        return None
    normalized = " ".join(show_name.split()).strip().lower()
    if not normalized:
        return None
    prefix = "the real housewives of "
    if normalized.startswith(prefix):
        location = normalized[len(prefix) :].strip()
        return _REAL_HOUSEWIVES_SHORT_CODE_BY_LOCATION.get(location)
    acronym_match = re.search(r"\bRHO(?:SLC|BH|NY|NJ|OC|DC|A|P|D|M)\b", show_name, re.IGNORECASE)
    if acronym_match:
        return acronym_match.group(0).upper()
    return None


def _fetch_imdb_title_fallback_metadata(
    imdb_title_ids: list[str],
) -> dict[str, dict[str, Any]]:
    if not imdb_title_ids:
        return {}
    from trr_backend.integrations.imdb.title_page_metadata import fetch_imdb_title_html, parse_imdb_title_html

    out: dict[str, dict[str, Any]] = {}
    for imdb_title_id in imdb_title_ids:
        title_id = str(imdb_title_id or "").strip()
        if not title_id:
            continue
        try:
            html = fetch_imdb_title_html(title_id, timeout_seconds=20.0)
            parsed = parse_imdb_title_html(html, imdb_id=title_id)
        except Exception as exc:  # noqa: BLE001
            logger.debug("IMDb title fallback fetch failed imdb_id=%s error=%s", title_id, exc)
            continue

        title_type = str(parsed.get("title_type") or "").strip()
        episode_title = str(parsed.get("title") or "").strip() or None
        season_number = _to_int(parsed.get("season_number"))
        episode_number = _to_int(parsed.get("episode_number"))
        show_name = str(parsed.get("series_title") or "").strip() or None
        show_imdb_id = str(parsed.get("series_imdb_id") or "").strip() or None
        episode_air_date = str(parsed.get("episode_air_date") or "").strip() or None
        if title_type.upper() == "TVEPISODE" or season_number is not None or episode_number is not None or show_name:
            out[title_id] = {
                "episode_imdb_id": title_id,
                "episode_title": episode_title,
                "season_number": season_number,
                "episode_number": episode_number,
                "episode_air_date": episode_air_date,
                "show_name": show_name,
                "show_imdb_id": show_imdb_id,
                "show_short_code": _derive_real_housewives_short_code(show_name),
                "imdb_title_type": title_type or None,
            }
    return out


def _normalize_show_lookup_key(value: str | None) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _strip_parenthetical_text(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = re.sub(r"\([^)]*\)", " ", value)
    stripped = " ".join(stripped.split()).strip()
    return stripped or None


def _iter_normalized_show_lookup_keys(value: str | None) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    candidates = [raw]
    stripped = _strip_parenthetical_text(raw)
    if stripped and stripped.casefold() != raw.casefold():
        candidates.append(stripped)
    out: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = _normalize_show_lookup_key(candidate)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _find_show_row_by_alias(by_alias: dict[str, dict[str, Any]], show_name: str | None) -> dict[str, Any] | None:
    for lookup_key in _iter_normalized_show_lookup_keys(show_name):
        row = by_alias.get(lookup_key)
        if isinstance(row, dict):
            return row
    return None


def _iter_show_aliases(show_row: dict[str, Any]) -> list[str]:
    aliases: list[str] = []
    show_name = show_row.get("name")
    if isinstance(show_name, str) and show_name.strip():
        aliases.append(show_name.strip())
    alternative_names = show_row.get("alternative_names")
    if isinstance(alternative_names, list):
        for alias in alternative_names:
            if isinstance(alias, str) and alias.strip():
                aliases.append(alias.strip())
    return aliases


def _build_show_lookup_maps(
    db: SupabaseAdminClient,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_imdb_id: dict[str, dict[str, Any]] = {}
    by_alias: dict[str, dict[str, Any]] = {}
    by_show_id: dict[str, dict[str, Any]] = {}
    try:
        response = db.schema("core").table("shows").select("id,name,imdb_id,alternative_names").execute()
    except Exception as exc:  # noqa: BLE001
        logger.debug("Show lookup bootstrap failed: %s", exc)
        return by_imdb_id, by_alias, by_show_id
    if hasattr(response, "error") and response.error:
        logger.debug("Show lookup bootstrap error: %s", response.error)
        return by_imdb_id, by_alias, by_show_id

    for row in response.data or []:
        if not isinstance(row, dict):
            continue
        show_id = str(row.get("id") or "").strip()
        show_imdb_id = str(row.get("imdb_id") or "").strip()
        if show_id:
            by_show_id[show_id] = row
        if show_imdb_id:
            by_imdb_id[show_imdb_id] = row
        for alias in _iter_show_aliases(row):
            for normalized in _iter_normalized_show_lookup_keys(alias):
                if normalized not in by_alias:
                    by_alias[normalized] = row

    try:
        alt_names_response = db.schema("core").table("show_alternative_names").select("show_id,name").execute()
    except Exception as exc:  # noqa: BLE001
        logger.debug("Show lookup alternative-names bootstrap failed: %s", exc)
        alt_names_response = None
    if alt_names_response is not None and not (hasattr(alt_names_response, "error") and alt_names_response.error):
        alt_rows = alt_names_response.data if isinstance(getattr(alt_names_response, "data", None), list) else []
        for alt_row in alt_rows:
            if not isinstance(alt_row, dict):
                continue
            show_id = str(alt_row.get("show_id") or "").strip()
            alt_name = str(alt_row.get("name") or "").strip()
            if not show_id or not alt_name:
                continue
            show_row = by_show_id.get(show_id)
            if not isinstance(show_row, dict):
                continue
            for normalized in _iter_normalized_show_lookup_keys(alt_name):
                if normalized not in by_alias:
                    by_alias[normalized] = show_row

    try:
        external_ids_response = (
            db.schema("core")
            .table("show_external_ids")
            .select("show_id,source_id,external_id")
            .eq("source_id", "imdb")
            .execute()
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("Show lookup external-id bootstrap failed: %s", exc)
        external_ids_response = None
    has_external_ids_error = bool(
        external_ids_response is not None
        and hasattr(external_ids_response, "error")
        and external_ids_response.error
    )
    if external_ids_response is not None and not has_external_ids_error:
        external_id_rows = (
            external_ids_response.data if isinstance(getattr(external_ids_response, "data", None), list) else []
        )
        for ext_row in external_id_rows:
            if not isinstance(ext_row, dict):
                continue
            if str(ext_row.get("source_id") or "").strip().lower() != "imdb":
                continue
            show_id = str(ext_row.get("show_id") or "").strip()
            external_id = str(ext_row.get("external_id") or "").strip()
            if not show_id or not external_id:
                continue
            show_row = by_show_id.get(show_id)
            if not isinstance(show_row, dict):
                continue
            by_imdb_id.setdefault(external_id, show_row)

    return by_imdb_id, by_alias, by_show_id


def _lookup_show_ids_by_name(db: SupabaseAdminClient, show_names: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    _, by_alias, _ = _build_show_lookup_maps(db)
    for raw_name in show_names:
        show_name = str(raw_name or "").strip()
        if not show_name or show_name in mapping:
            continue
        match = _find_show_row_by_alias(by_alias, show_name)
        show_id = str(match.get("id") or "").strip() if isinstance(match, dict) else ""
        if show_id:
            mapping[show_name] = show_id
    return mapping


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

    unresolved_ids = [imdb_id for imdb_id in imdb_ids if imdb_id not in episodes_by_imdb]
    imdb_fallback_by_id = _fetch_imdb_title_fallback_metadata(unresolved_ids)
    show_lookup_by_imdb_id, show_lookup_by_alias, show_lookup_by_id = _build_show_lookup_maps(db)

    if not episodes_by_imdb and not imdb_fallback_by_id:
        return tagged, failed

    for row in photos:
        if row.get("source") != "imdb":
            continue
        title_ids = row.get("title_imdb_ids") or []
        if not isinstance(title_ids, list):
            continue
        episode: dict[str, Any] | None = None
        fallback: dict[str, Any] | None = None
        for imdb_id in title_ids:
            if imdb_id in episodes_by_imdb:
                episode = episodes_by_imdb[imdb_id]
                break
            if imdb_id in imdb_fallback_by_id:
                fallback = imdb_fallback_by_id[imdb_id]
                break
        if not episode and not fallback:
            continue

        metadata = dict(row.get("metadata") or {})
        if episode:
            episode_show_id = str(episode.get("show_id") or "").strip() or None
            resolved_show_row = show_lookup_by_id.get(episode_show_id) if episode_show_id else None
            resolved_show_name = (
                str(resolved_show_row.get("name") or "").strip()
                if isinstance(resolved_show_row, dict)
                else str(episode.get("show_name") or "").strip()
            ) or None
            resolved_show_imdb_id = (
                str(resolved_show_row.get("imdb_id") or "").strip() if isinstance(resolved_show_row, dict) else ""
            ) or None
            metadata.update(
                {
                    "episode_id": episode.get("id"),
                    "episode_imdb_id": episode.get("imdb_episode_id"),
                    "episode_title": episode.get("title"),
                    "episode_number": episode.get("episode_number"),
                    "season_number": episode.get("season_number"),
                    "episode_air_date": episode.get("air_date"),
                    "imdb_fallback_show_name": None,
                    "imdb_fallback_show_imdb_id": None,
                    "show_id": episode_show_id,
                    "show_name": resolved_show_name,
                    "show_imdb_id": resolved_show_imdb_id,
                    "source_created_at": episode.get("air_date"),
                    "show_context_source": "episode_table",
                }
            )
            if not metadata.get("show_short_code"):
                metadata["show_short_code"] = _derive_real_housewives_short_code(resolved_show_name)
        elif fallback:
            show_name = str(fallback.get("show_name") or "").strip() or None
            show_imdb_id = str(fallback.get("show_imdb_id") or "").strip() or None
            resolved_show_row: dict[str, Any] | None = None
            if show_imdb_id and show_imdb_id in show_lookup_by_imdb_id:
                resolved_show_row = show_lookup_by_imdb_id.get(show_imdb_id)
            if not resolved_show_row and show_name:
                resolved_show_row = _find_show_row_by_alias(show_lookup_by_alias, show_name)
            resolved_show_id = (
                str(resolved_show_row.get("id") or "").strip() if isinstance(resolved_show_row, dict) else ""
            ) or None
            resolved_show_name = (
                str(resolved_show_row.get("name") or "").strip() if isinstance(resolved_show_row, dict) else ""
            ) or None
            resolved_show_imdb_id = (
                str(resolved_show_row.get("imdb_id") or "").strip() if isinstance(resolved_show_row, dict) else ""
            ) or show_imdb_id
            resolved_show_short_code = _derive_real_housewives_short_code(resolved_show_name or show_name)
            metadata.update(
                {
                    "episode_imdb_id": fallback.get("episode_imdb_id"),
                    "episode_title": fallback.get("episode_title"),
                    "episode_number": fallback.get("episode_number"),
                    "season_number": fallback.get("season_number"),
                    "episode_air_date": fallback.get("episode_air_date"),
                    "imdb_fallback_show_name": show_name,
                    "imdb_fallback_show_imdb_id": show_imdb_id,
                    "show_name": resolved_show_name,
                    "show_imdb_id": resolved_show_imdb_id,
                    "show_short_code": resolved_show_short_code if resolved_show_id else None,
                    "imdb_title_type": fallback.get("imdb_title_type"),
                    "source_created_at": fallback.get("episode_air_date"),
                    "show_context_source": "imdb_title_fallback" if resolved_show_id else "imdb_episode_unresolved",
                }
            )
            metadata["show_id"] = resolved_show_id
            if not resolved_show_id:
                metadata["show_id"] = None
                metadata["show_name"] = None
                metadata["show_imdb_id"] = None
                metadata["show_short_code"] = None

            title_names = row.get("title_names")
            if isinstance(title_names, list):
                merged_titles: list[str] = []
                seen_titles: set[str] = set()
                for candidate in [*title_names, fallback.get("episode_title"), show_name]:
                    if not isinstance(candidate, str) or not candidate.strip():
                        continue
                    normalized = candidate.strip()
                    key = normalized.casefold()
                    if key in seen_titles:
                        continue
                    seen_titles.add(key)
                    merged_titles.append(normalized)
                if merged_titles:
                    row["title_names"] = merged_titles

        row["metadata"] = metadata
        season_number = metadata.get("season_number")
        if not row.get("season") and season_number is not None:
            row["season"] = season_number
        tagged += 1
    return tagged, failed


def _is_imdb_episode_or_title_evidence(
    row: dict[str, Any],
    metadata: dict[str, Any],
) -> bool:
    if str(row.get("source") or "").strip().lower() != "imdb":
        return False
    title_ids = row.get("title_imdb_ids")
    if isinstance(title_ids, list) and any(isinstance(item, str) and item.strip() for item in title_ids):
        return True
    title_names = row.get("title_names")
    if isinstance(title_names, list) and any(isinstance(item, str) and item.strip() for item in title_names):
        return True
    if isinstance(metadata.get("episode_imdb_id"), str) and str(metadata.get("episode_imdb_id")).strip():
        return True
    if isinstance(metadata.get("episode_title"), str) and str(metadata.get("episode_title")).strip():
        return True
    imdb_title_type = str(metadata.get("imdb_title_type") or "").strip().upper()
    if imdb_title_type == "TVEPISODE":
        return True
    return False


def _build_episode_title_lookup_rows(
    db: SupabaseAdminClient,
    show_id: str,
) -> list[dict[str, Any]]:
    try:
        response = (
            db.schema("core")
            .table("episodes")
            .select("title,season_number,episode_number")
            .eq("show_id", show_id)
            .execute()
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("Episode title lookup failed show_id=%s error=%s", show_id, exc)
        return []
    if hasattr(response, "error") and response.error:
        logger.debug("Episode title lookup error show_id=%s error=%s", show_id, response.error)
        return []
    rows = response.data
    return rows if isinstance(rows, list) else []


def _title_lookup_key(value: str | None) -> str:
    normalized = _normalize_show_lookup_key(value)
    if normalized:
        return normalized
    return ""


def _try_infer_show_context_for_unresolved_imdb_episode(
    row: dict[str, Any],
    metadata: dict[str, Any],
    *,
    show_id: str | None,
    show_name: str | None,
    show_imdb_id: str | None,
    show_lookup_by_alias: dict[str, dict[str, Any]],
    requested_show_episode_rows: list[dict[str, Any]],
) -> bool:
    if not show_id and not show_name:
        return False
    fallback_show_imdb_id = str(metadata.get("imdb_fallback_show_imdb_id") or "").strip() or None
    fallback_show_name = str(metadata.get("imdb_fallback_show_name") or "").strip() or None

    if fallback_show_imdb_id and show_imdb_id and fallback_show_imdb_id.casefold() == show_imdb_id.casefold():
        return True

    fallback_show_row = _find_show_row_by_alias(show_lookup_by_alias, fallback_show_name)
    fallback_show_id = str(fallback_show_row.get("id") or "").strip() if isinstance(fallback_show_row, dict) else ""
    if fallback_show_id and show_id and fallback_show_id == show_id:
        return True

    episode_title = str(metadata.get("episode_title") or "").strip()
    if not episode_title:
        title_names = row.get("title_names")
        if isinstance(title_names, list):
            for candidate in title_names:
                if isinstance(candidate, str) and candidate.strip():
                    episode_title = candidate.strip()
                    break
    if not episode_title:
        return False

    lookup_title = _title_lookup_key(episode_title)
    if not lookup_title:
        return False

    season_number = _to_int(metadata.get("season_number"))
    episode_number = _to_int(metadata.get("episode_number"))
    rows = requested_show_episode_rows
    if not rows:
        return False

    if season_number is not None and episode_number is not None:
        for episode_row in rows:
            if not isinstance(episode_row, dict):
                continue
            if _title_lookup_key(str(episode_row.get("title") or "")) != lookup_title:
                continue
            if _to_int(episode_row.get("season_number")) != season_number:
                continue
            if _to_int(episode_row.get("episode_number")) != episode_number:
                continue
            return True

    for episode_row in rows:
        if not isinstance(episode_row, dict):
            continue
        if _title_lookup_key(str(episode_row.get("title") or "")) == lookup_title:
            return True
    return False


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
    show_lookup_by_imdb_id, show_lookup_by_alias, show_lookup_by_id = _build_show_lookup_maps(db)
    requested_show_row: dict[str, Any] | None = None
    if show_id_str:
        requested_show_row = show_lookup_by_id.get(show_id_str)
    if not requested_show_row and show_name_val:
        requested_show_row = _find_show_row_by_alias(show_lookup_by_alias, show_name_val)
        if isinstance(requested_show_row, dict) and not show_id_str:
            show_id_candidate = str(requested_show_row.get("id") or "").strip()
            show_id_str = show_id_candidate or show_id_str
    if isinstance(requested_show_row, dict) and not show_name_val:
        show_name_candidate = str(requested_show_row.get("name") or "").strip()
        if show_name_candidate:
            show_name_val = show_name_candidate
    requested_show_imdb_id = (
        str(requested_show_row.get("imdb_id") or "").strip() if isinstance(requested_show_row, dict) else ""
    ) or None
    requested_show_episode_rows = _build_episode_title_lookup_rows(db, show_id_str) if show_id_str else []

    for row in photos:
        metadata = dict(row.get("metadata") or {})
        before_show_id = metadata.get("show_id")
        before_show_name = metadata.get("show_name")
        show_context_source = str(metadata.get("show_context_source") or "").strip().lower()

        # Only apply requested show context when this photo has no show metadata.
        has_show_metadata = (
            bool(before_show_id and isinstance(before_show_id, str) and before_show_id.strip())
            or bool(before_show_name and isinstance(before_show_name, str) and before_show_name.strip())
        )
        has_unresolved_imdb_episode_evidence = (
            show_context_source == "imdb_episode_unresolved"
            or (
                not has_show_metadata
                and _is_imdb_episode_or_title_evidence(row, metadata)
            )
        )
        if not has_show_metadata:
            if has_unresolved_imdb_episode_evidence:
                inferred_show_context_applied = False
                if _try_infer_show_context_for_unresolved_imdb_episode(
                    row,
                    metadata,
                    show_id=show_id_str,
                    show_name=show_name_val,
                    show_imdb_id=requested_show_imdb_id,
                    show_lookup_by_alias=show_lookup_by_alias,
                    requested_show_episode_rows=requested_show_episode_rows,
                ):
                    if show_id_str:
                        metadata["show_id"] = show_id_str
                    if show_name_val:
                        metadata["show_name"] = show_name_val
                    if requested_show_imdb_id:
                        metadata["show_imdb_id"] = requested_show_imdb_id
                    if not metadata.get("show_short_code"):
                        metadata["show_short_code"] = _derive_real_housewives_short_code(show_name_val)
                    metadata["show_context_source"] = "request_context_inferred"
                    inferred_show_context_applied = True
                row["metadata"] = metadata
                if inferred_show_context_applied and (
                    metadata.get("show_id") != before_show_id or metadata.get("show_name") != before_show_name
                ):
                    tagged += 1
                continue
            if show_id_str:
                metadata["show_id"] = show_id_str
            if show_name_val:
                metadata["show_name"] = show_name_val
            if metadata.get("show_id") or metadata.get("show_name"):
                metadata["show_context_source"] = "request_context"

        row["metadata"] = metadata
        if metadata.get("show_id") != before_show_id or metadata.get("show_name") != before_show_name:
            tagged += 1
    return tagged, failed


def _load_existing_imdb_cast_photos_for_person(
    db: SupabaseAdminClient,
    person_id: str,
) -> list[dict[str, Any]]:
    def _select_existing_rows(select_columns: str) -> Any:
        return (
            db.schema("core")
            .table("cast_photos")
            .select(select_columns)
            .eq("person_id", person_id)
            .eq("source", "imdb")
            .execute()
        )

    def _is_missing_column_error(error: Any, column: str) -> bool:
        text = str(error).lower()
        return column in text and "column" in text and "does not exist" in text

    select_columns_with_source_asset = (
        "id,person_id,imdb_person_id,source,source_image_id,viewer_id,source_asset_id,source_page_url,"
        "image_url,image_url_canonical,url,thumb_url,caption,width,height,season,context_section,"
        "context_type,people_names,people_imdb_ids,title_names,title_imdb_ids,metadata"
    )
    select_columns_without_source_asset = (
        "id,person_id,imdb_person_id,source,source_image_id,viewer_id,source_page_url,"
        "image_url,image_url_canonical,url,thumb_url,caption,width,height,season,context_section,"
        "context_type,people_names,people_imdb_ids,title_names,title_imdb_ids,metadata"
    )

    try:
        response = _select_existing_rows(select_columns_with_source_asset)
    except Exception as exc:  # noqa: BLE001
        if _is_missing_column_error(exc, "source_asset_id"):
            logger.warning(
                "Existing IMDb photo lookup fallback (missing source_asset_id) person_id=%s error=%s",
                person_id,
                exc,
            )
            try:
                response = _select_existing_rows(select_columns_without_source_asset)
            except Exception as fallback_exc:  # noqa: BLE001
                logger.warning("Existing IMDb photo lookup failed person_id=%s error=%s", person_id, fallback_exc)
                return []
        else:
            logger.warning("Existing IMDb photo lookup failed person_id=%s error=%s", person_id, exc)
            return []

    if hasattr(response, "error") and response.error:
        if _is_missing_column_error(response.error, "source_asset_id"):
            logger.warning(
                "Existing IMDb photo lookup fallback from response error (missing source_asset_id) "
                "person_id=%s error=%s",
                person_id,
                response.error,
            )
            try:
                response = _select_existing_rows(select_columns_without_source_asset)
            except Exception as fallback_exc:  # noqa: BLE001
                logger.warning("Existing IMDb photo lookup failed person_id=%s error=%s", person_id, fallback_exc)
                return []
            if hasattr(response, "error") and response.error:
                logger.warning("Existing IMDb photo lookup error person_id=%s error=%s", person_id, response.error)
                return []
        else:
            logger.warning("Existing IMDb photo lookup error person_id=%s error=%s", person_id, response.error)
            return []

    rows = response.data or []
    if not isinstance(rows, list):
        return []
    for row in rows:
        if isinstance(row, dict):
            row.setdefault("source_asset_id", None)
            if "people_ids" not in row and isinstance(row.get("people_imdb_ids"), list):
                row["people_ids"] = list(row.get("people_imdb_ids") or [])
    return rows


def _extract_imdb_viewer_id_from_row(row: dict[str, Any]) -> str | None:
    viewer_id = str(row.get("viewer_id") or "").strip()
    if viewer_id:
        return viewer_id
    source_page_url = str(row.get("source_page_url") or "").strip()
    if not source_page_url:
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        source_page_url = str(metadata.get("source_page_url") or "").strip() if isinstance(metadata, dict) else ""
    if not source_page_url:
        return None
    match = IMDB_VIEWER_ID_RE.search(source_page_url)
    return match.group(1) if match else None


def _needs_imdb_metadata_refresh(row: dict[str, Any]) -> bool:
    if str(row.get("source") or "").strip().lower() != "imdb":
        return False
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    title_ids = row.get("title_imdb_ids") if isinstance(row.get("title_imdb_ids"), list) else []
    people_ids = row.get("people_imdb_ids") if isinstance(row.get("people_imdb_ids"), list) else []
    tags = metadata.get("tags") if isinstance(metadata, dict) else None
    has_tags = isinstance(tags, dict) and bool(tags)
    imdb_image_type = metadata.get("imdb_image_type")
    has_image_type = isinstance(imdb_image_type, str) and bool(imdb_image_type.strip())
    unresolved_show = str(metadata.get("show_context_source") or "").strip().lower() == "imdb_episode_unresolved"
    return (not title_ids) or (not people_ids) or (not has_tags) or (not has_image_type) or unresolved_show


def _load_imdb_viewer_image_types(
    imdb_person_id: str,
    viewer_ids: set[str],
) -> dict[str, str]:
    from trr_backend.integrations.imdb.person_gallery import (
        fetch_imdb_person_mediaindex_html,
        fetch_imdb_person_mediaindex_page,
        parse_imdb_person_mediaindex_payload,
        parse_imdb_person_mediaindex_state,
    )

    normalized_targets = {str(viewer_id or "").strip() for viewer_id in viewer_ids if str(viewer_id or "").strip()}
    if not imdb_person_id or not normalized_targets:
        return {}
    out: dict[str, str] = {}
    try:
        html = fetch_imdb_person_mediaindex_html(imdb_person_id, session=None)
        images, page_info = parse_imdb_person_mediaindex_state(html, imdb_person_id)
    except Exception:  # noqa: BLE001
        return {}

    def _add_images(rows: list[dict[str, Any]]) -> None:
        for image in rows:
            viewer_id = str(image.get("viewer_id") or "").strip()
            image_type = str(image.get("image_type") or "").strip().lower()
            if viewer_id in normalized_targets and image_type:
                out[viewer_id] = image_type

    _add_images(images)
    if len(out) >= len(normalized_targets):
        return out

    cursor = page_info.get("end_cursor")
    has_next = bool(page_info.get("has_next_page"))
    pages_fetched = 1
    max_pages = 10
    while has_next and cursor and pages_fetched < max_pages and len(out) < len(normalized_targets):
        try:
            payload = fetch_imdb_person_mediaindex_page(imdb_person_id, after_cursor=cursor, first=50, session=None)
            next_images, next_page_info = parse_imdb_person_mediaindex_payload(payload, imdb_person_id)
        except Exception:  # noqa: BLE001
            break
        _add_images(next_images)
        pages_fetched += 1
        next_cursor = next_page_info.get("end_cursor")
        has_next = bool(next_page_info.get("has_next_page"))
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    return out


def _apply_traitors_filter_metadata_to_existing_row(
    row: dict[str, Any],
    *,
    strict_context: dict[str, Any],
) -> None:
    if not bool(strict_context.get("strict_mode_enabled")):
        return
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    image_type = str(metadata.get("imdb_image_type") or "").strip().lower()
    strict_types = {
        str(v or "").strip().lower() for v in (strict_context.get("strict_types") or set()) if str(v or "").strip()
    }
    if strict_types and image_type not in strict_types:
        return

    target_imdb_id = str(strict_context.get("target_person_imdb_id") or "").strip().lower() or None
    target_name = _normalize_name_for_match(strict_context.get("target_person_name"))
    allowed_cast_imdb_ids = {
        str(v or "").strip().lower()
        for v in (strict_context.get("allowed_cast_imdb_ids") or set())
        if str(v or "").strip()
    }
    allowed_cast_names = {
        _normalize_name_for_match(v)
        for v in (strict_context.get("allowed_cast_names") or set())
        if isinstance(v, str) and v.strip()
    }
    title_ids = {
        str(v or "").strip().lower() for v in (row.get("title_imdb_ids") or []) if isinstance(v, str) and v.strip()
    }
    people_ids = {
        str(v or "").strip().lower() for v in (row.get("people_imdb_ids") or []) if isinstance(v, str) and v.strip()
    }
    people_names = {
        _normalize_name_for_match(v) for v in (row.get("people_names") or []) if isinstance(v, str) and v.strip()
    }
    episode_ids = {
        str(v or "").strip().lower()
        for v in (strict_context.get("allowed_episode_imdb_ids") or set())
        if str(v or "").strip()
    }

    reason: str | None = None
    if target_imdb_id and people_ids and len(people_ids) == 1 and target_imdb_id in people_ids:
        reason = "solo_self"
    elif target_name and people_names and len(people_names) == 1 and target_name in people_names:
        reason = "solo_self"
    elif target_imdb_id and people_ids and allowed_cast_imdb_ids:
        if len(people_ids) >= 2 and target_imdb_id in people_ids and people_ids.issubset(allowed_cast_imdb_ids):
            reason = "traitors_cast_group"
    elif target_name and people_names and allowed_cast_names:
        if len(people_names) >= 2 and target_name in people_names and people_names.issubset(allowed_cast_names):
            reason = "traitors_cast_group"
    if reason is None and image_type == "still_frame" and title_ids and episode_ids.intersection(title_ids):
        reason = "episode_still_frame"
    if reason:
        metadata["imdb_filter_scope"] = "traitors_strict"
        metadata["imdb_filter_reason"] = reason
    row["metadata"] = metadata


def _repair_existing_imdb_cast_photos(
    db: SupabaseAdminClient,
    person_id: str,
    *,
    show_id: UUID | None,
    show_name: str | None,
    strict_context: dict[str, Any] | None = None,
) -> tuple[int, int]:
    from trr_backend.integrations.imdb.person_gallery import (
        fetch_imdb_person_mediaviewer_html,
        parse_imdb_person_mediaviewer_details,
    )
    from trr_backend.repositories.cast_photos import upsert_cast_photos

    rows = _load_existing_imdb_cast_photos_for_person(db, person_id)
    if not rows:
        return 0, 0

    repair_rows = [row for row in rows if _needs_imdb_metadata_refresh(row)]
    if not repair_rows:
        return 0, 0

    metadata_failures = 0
    imdb_person_id = ""
    for row in repair_rows:
        imdb_person_id = str(row.get("imdb_person_id") or "").strip()
        if imdb_person_id:
            break

    viewer_ids = {
        viewer_id
        for viewer_id in [_extract_imdb_viewer_id_from_row(row) for row in repair_rows]
        if isinstance(viewer_id, str) and viewer_id.strip()
    }
    image_type_lookup = _load_imdb_viewer_image_types(imdb_person_id, viewer_ids) if imdb_person_id else {}

    refreshed_at = datetime.now(UTC).isoformat()
    for row in repair_rows:
        metadata = dict(row.get("metadata") or {})
        viewer_id = _extract_imdb_viewer_id_from_row(row)
        if viewer_id:
            metadata["imdb_viewer_id"] = viewer_id
            image_type = image_type_lookup.get(viewer_id)
            if image_type:
                metadata["imdb_image_type"] = image_type

        row_imdb_person_id = str(row.get("imdb_person_id") or "").strip() or imdb_person_id
        if viewer_id and row_imdb_person_id:
            try:
                viewer_html = fetch_imdb_person_mediaviewer_html(row_imdb_person_id, viewer_id, session=None)
                details = parse_imdb_person_mediaviewer_details(viewer_html, viewer_id=viewer_id)
            except Exception as exc:  # noqa: BLE001
                metadata_failures += 1
                logger.debug(
                    "Existing IMDb metadata refresh failed person_id=%s viewer_id=%s error=%s",
                    person_id,
                    viewer_id,
                    exc,
                )
                details = {}

            if details:
                caption = details.get("caption")
                if isinstance(caption, str) and caption.strip():
                    row["caption"] = caption.strip()
                details_image_type = details.get("image_type")
                if isinstance(details_image_type, str) and details_image_type.strip():
                    metadata["imdb_image_type"] = details_image_type.strip().lower()
                people_ids = details.get("people_imdb_ids")
                if isinstance(people_ids, list):
                    row["people_imdb_ids"] = [str(v).strip() for v in people_ids if isinstance(v, str) and v.strip()]
                people_names = details.get("people_names")
                if isinstance(people_names, list):
                    row["people_names"] = [str(v).strip() for v in people_names if isinstance(v, str) and v.strip()]
                title_ids = details.get("title_imdb_ids")
                if isinstance(title_ids, list):
                    row["title_imdb_ids"] = [str(v).strip() for v in title_ids if isinstance(v, str) and v.strip()]
                title_names = details.get("title_names")
                if isinstance(title_names, list):
                    row["title_names"] = [str(v).strip() for v in title_names if isinstance(v, str) and v.strip()]

                tags: dict[str, Any] = {}
                if isinstance(row.get("people_imdb_ids"), list):
                    ids = row.get("people_imdb_ids") or []
                    names = row.get("people_names") if isinstance(row.get("people_names"), list) else []
                    tags["people"] = [
                        {
                            "imdb_id": ids[idx],
                            "name": names[idx] if idx < len(names) else None,
                        }
                        for idx in range(len(ids))
                    ]
                if isinstance(row.get("title_imdb_ids"), list):
                    ids = row.get("title_imdb_ids") or []
                    names = row.get("title_names") if isinstance(row.get("title_names"), list) else []
                    tags["titles"] = [
                        {
                            "imdb_id": ids[idx],
                            "title": names[idx] if idx < len(names) else None,
                        }
                        for idx in range(len(ids))
                    ]
                if isinstance(row.get("caption"), str) and row.get("caption"):
                    tags["caption_plain"] = row.get("caption")
                if tags:
                    metadata["tags"] = tags
        metadata["imdb_metadata_refreshed_at"] = refreshed_at
        row["metadata"] = metadata
        if isinstance(strict_context, dict) and strict_context:
            _apply_traitors_filter_metadata_to_existing_row(row, strict_context=strict_context)

    try:
        _, episode_failed = _enrich_cast_photos_with_episode_metadata(db, repair_rows)
        metadata_failures += episode_failed
    except Exception as exc:  # noqa: BLE001
        metadata_failures += 1
        logger.warning("Existing IMDb repair enrichment failed person_id=%s error=%s", person_id, exc)

    try:
        _, show_failed = _apply_show_context_to_photos(
            db,
            repair_rows,
            show_id=show_id,
            show_name=show_name,
        )
        metadata_failures += show_failed
    except Exception as exc:  # noqa: BLE001
        metadata_failures += 1
        logger.warning("Existing IMDb repair show-context failed person_id=%s error=%s", person_id, exc)

    repair_rows = [
        row
        for row in repair_rows
        if isinstance(row.get("source_image_id"), str) and str(row.get("source_image_id")).strip()
    ]
    if not repair_rows:
        return 0, metadata_failures

    try:
        upserted = upsert_cast_photos(db, repair_rows, dedupe_on="source_image_id")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Existing IMDb repair upsert failed person_id=%s error=%s", person_id, exc)
        return 0, metadata_failures + 1

    return len(upserted), metadata_failures


def _refresh_tmdb_profile(
    db: SupabaseAdminClient,
    person_id: str,
    *,
    tmdb_person_id: int | None,
) -> Literal["updated", "not_found", "skipped"]:
    if not tmdb_person_id:
        return "skipped"
    from trr_backend.integrations.tmdb_person import fetch_tmdb_person_full
    from trr_backend.repositories.cast_tmdb import upsert_cast_tmdb

    person_full = fetch_tmdb_person_full(int(tmdb_person_id))
    if not person_full:
        return "not_found"
    upsert_cast_tmdb(db, person_full.to_cast_tmdb_row(person_id))
    return "updated"


def _classify_tmdb_profile_error(exc: Exception) -> str:
    request_exception_type: type[Exception]
    try:
        import requests

        request_exception_type = requests.RequestException
    except Exception:  # noqa: BLE001
        request_exception_type = Exception  # type: ignore[assignment]
    cast_tmdb_repository_error_type: type[Exception]
    try:
        from trr_backend.repositories import cast_tmdb as cast_tmdb_repo

        cast_tmdb_repository_error_type = cast_tmdb_repo.CastTMDbRepositoryError
    except Exception:  # noqa: BLE001
        cast_tmdb_repository_error_type = RuntimeError  # type: ignore[assignment]

    if isinstance(exc, cast_tmdb_repository_error_type):
        return "CAST_TMDB_UPSERT_FAILED"
    if isinstance(exc, request_exception_type):
        return "TMDB_FETCH_FAILED"
    return "TMDB_PROFILE_REFRESH_FAILED"


def _run_tmdb_profile_refresh(
    db: SupabaseAdminClient,
    person_id: str,
    *,
    tmdb_person_id: int | None,
) -> tuple[Literal["ok", "skipped", "failed"], str | None, str | None]:
    if not tmdb_person_id:
        return "skipped", "TMDB_ID_MISSING", "No TMDb person ID was available."

    try:
        result = _refresh_tmdb_profile(db, person_id, tmdb_person_id=tmdb_person_id)
    except Exception as exc:  # noqa: BLE001
        return "failed", _classify_tmdb_profile_error(exc), str(exc)

    if result == "not_found":
        return "skipped", "TMDB_NOT_FOUND", f"TMDb person {tmdb_person_id} was not found."
    if result == "skipped":
        return "skipped", "TMDB_ID_MISSING", "No TMDb person ID was available."
    return "ok", None, None


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
        and (expected_first.startswith(candidate_first) or candidate_first.startswith(expected_first))
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
    asset_ids: list[str] | None = None,
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
        allowed_asset_ids = set(asset_ids or [])
        for row in rows:
            asset_id = str(row.get("media_asset_id") or "")
            if not asset_id or asset_id in seen_asset_ids:
                continue
            if allowed_asset_ids and asset_id not in allowed_asset_ids:
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
    sources, fandom_skipped = _resolve_refresh_sources(db, request)
    errors: list[str] = []
    tmdb_profile_status: Literal["ok", "skipped", "failed"] | None = None
    tmdb_profile_error_code: str | None = None
    tmdb_profile_error_detail: str | None = None
    if fandom_skipped:
        errors.append("Fandom sources skipped for non-Real Housewives show context.")

    # 1.5 Refresh person profiles (best-effort)
    tmdb_profile_status, tmdb_profile_error_code, tmdb_profile_error_detail = _run_tmdb_profile_refresh(
        db,
        person_id_str,
        tmdb_person_id=tmdb_person_id,
    )
    if tmdb_profile_status == "failed":
        logger.warning("TMDb profile refresh failed for %s: %s", person_id, tmdb_profile_error_detail)
        errors.append(f"TMDb profile [{tmdb_profile_error_code}]: {tmdb_profile_error_detail}")
    if "fandom" in sources or "fandom-gallery" in sources:
        try:
            _refresh_fandom_profile(db, person_id_str, person_name=person_name)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Fandom profile refresh failed for %s: %s", person_id, exc)
            errors.append(f"Fandom profile: {exc}")

    imdb_allowed_title_ids, imdb_allowed_keywords, imdb_prioritize_solo = _resolve_imdb_focus_filters(
        db,
        request.show_id,
        request.show_name,
    )
    imdb_strict_context = _resolve_imdb_traitors_strict_context(
        db,
        show_id=request.show_id,
        show_name=request.show_name,
        target_person_imdb_id=imdb_person_id,
        target_person_name=person_name,
    )
    imdb_diagnostics = _empty_imdb_refresh_diagnostics()

    # 2. Fetch photos
    try:
        photos = fetch_all_cast_photos(
            person_id_str,
            imdb_person_id=imdb_person_id,
            tmdb_person_id=tmdb_person_id,
            person_name=person_name,
            sources=list(sources),
            limit_per_source=request.limit_per_source,
            imdb_allowed_title_imdb_ids=imdb_allowed_title_ids,
            imdb_allowed_title_keywords=imdb_allowed_keywords,
            imdb_prioritize_solo_people=imdb_prioritize_solo,
            imdb_strict_types=set(imdb_strict_context.get("strict_types") or set()),
            imdb_target_person_imdb_id=imdb_strict_context.get("target_person_imdb_id"),
            imdb_target_person_name=imdb_strict_context.get("target_person_name"),
            imdb_allowed_cast_imdb_ids=set(imdb_strict_context.get("allowed_cast_imdb_ids") or set()),
            imdb_allowed_cast_names=set(imdb_strict_context.get("allowed_cast_names") or set()),
            imdb_allowed_episode_imdb_ids=set(imdb_strict_context.get("allowed_episode_imdb_ids") or set()),
            imdb_strict_mode_enabled=bool(imdb_strict_context.get("strict_mode_enabled")),
            imdb_diagnostics=imdb_diagnostics,
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

    # 3.5 Repair existing IMDb rows for this person so refresh is self-healing.
    existing_imdb_rows_repaired = 0
    try:
        existing_imdb_rows_repaired, existing_imdb_repair_failed = _repair_existing_imdb_cast_photos(
            db,
            person_id_str,
            show_id=request.show_id,
            show_name=request.show_name,
            strict_context=imdb_strict_context,
        )
        metadata_enrichment_failed += existing_imdb_repair_failed
        if existing_imdb_rows_repaired > 0:
            logger.info(
                "Existing IMDb rows repaired person_id=%s repaired=%s",
                person_id_str,
                existing_imdb_rows_repaired,
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Existing IMDb repair stage failed for %s: %s", person_id, exc)
        metadata_enrichment_failed += 1
        errors.append(f"IMDb repair: {exc}")

    # 4. Mirror to S3
    cast_photos_mirrored, cast_photos_failed = 0, 0
    media_assets_mirrored, media_assets_failed = 0, 0
    if not request.skip_mirror:
        try:
            cast_photos_mirrored, cast_photos_failed = _mirror_person_photos(
                db, person_id_str, imdb_person_id, force=request.force_mirror
            )
            media_assets_mirrored, media_assets_failed = _mirror_person_media_assets(
                db, person_id_str, force=request.force_mirror
            )
        except Exception as exc:
            logger.exception(f"Mirror error for {person_id}")
            errors.append(f"Mirror: {exc}")
    photos_mirrored = cast_photos_mirrored + media_assets_mirrored
    photos_failed = cast_photos_failed + media_assets_failed

    # 4.5 Auto-count people for newly upserted TMDb/Fandom photos (only when no manual tags)
    auto_counts_attempted = 0
    auto_counts_succeeded = 0
    auto_counts_failed = 0
    if not request.skip_auto_count:
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
    text_overlay_attempted = 0
    text_overlay_succeeded = 0
    text_overlay_unknown = 0
    text_overlay_failed = 0
    text_overlay_reason_counts: dict[str, int] = {}
    if not request.skip_word_detection:
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
    centering_attempted = 0
    centering_succeeded = 0
    centering_failed = 0
    centering_skipped_manual = 0
    if not request.skip_centering:
        centering_attempted, centering_succeeded, centering_failed, centering_skipped_manual = (
            _recenter_person_gallery_images(
                db,
                person_id_str,
                sources,
                photo_ids=None,
                force=False,
            )
        )

    resize_attempted = 0
    resize_succeeded = 0
    resize_failed = 0
    resize_crop_attempted = 0
    resize_crop_succeeded = 0
    resize_crop_failed = 0
    if not request.skip_resize:
        (
            resize_attempted,
            resize_succeeded,
            resize_failed,
            resize_crop_attempted,
            resize_crop_succeeded,
            resize_crop_failed,
        ) = _resize_person_gallery_images(
            db,
            person_id_str,
            sources,
            force=False,
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
        tmdb_profile_status=tmdb_profile_status,
        tmdb_profile_error_code=tmdb_profile_error_code,
        tmdb_profile_error_detail=tmdb_profile_error_detail,
        sources_used=list(sources),
        photos_fetched=len(photos),
        photos_upserted=photos_upserted,
        photos_mirrored=photos_mirrored,
        photos_failed=photos_failed,
        cast_photos_mirrored=cast_photos_mirrored,
        cast_photos_failed=cast_photos_failed,
        media_assets_mirrored=media_assets_mirrored,
        media_assets_failed=media_assets_failed,
        photos_pruned=photos_pruned,
        imdb_pages_scanned=int(imdb_diagnostics.get("imdb_pages_scanned", 0)),
        imdb_candidates_seen=int(imdb_diagnostics.get("imdb_candidates_seen", 0)),
        imdb_kept=int(imdb_diagnostics.get("imdb_kept", 0)),
        imdb_filtered_type=int(imdb_diagnostics.get("imdb_filtered_type", 0)),
        imdb_filtered_people=int(imdb_diagnostics.get("imdb_filtered_people", 0)),
        imdb_filtered_episode=int(imdb_diagnostics.get("imdb_filtered_episode", 0)),
        imdb_filtered_other=int(imdb_diagnostics.get("imdb_filtered_other", 0)),
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
        resize_attempted=resize_attempted,
        resize_succeeded=resize_succeeded,
        resize_failed=resize_failed,
        resize_crop_attempted=resize_crop_attempted,
        resize_crop_succeeded=resize_crop_succeeded,
        resize_crop_failed=resize_crop_failed,
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
    from trr_backend.repositories.cast_photos import upsert_cast_photos

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
        photos_upserted = 0
        existing_imdb_rows_repaired = 0
        imdb_diagnostics = _empty_imdb_refresh_diagnostics()
        photos_mirrored = 0
        cast_photos_mirrored = 0
        media_assets_mirrored = 0
        auto_counts_succeeded = 0
        text_overlay_succeeded = 0
        centering_succeeded = 0
        resize_succeeded = 0
        tmdb_profile_status: Literal["ok", "skipped", "failed"] | None = None
        tmdb_profile_error_code: str | None = None
        tmdb_profile_error_detail: str | None = None

        def build_live_counts() -> dict[str, int]:
            return {
                "synced": int(photos_upserted),
                "mirrored": int(photos_mirrored),
                "counted": int(auto_counts_succeeded),
                "cropped": int(centering_succeeded),
                "id_text": int(text_overlay_succeeded),
                "resized": int(resize_succeeded),
            }

        def progress(payload: dict[str, Any]) -> str:
            return (
                "event: progress\ndata: "
                + json.dumps({"run_id": run_id, "live_counts": build_live_counts(), **payload})
                + "\n\n"
            )

        def error_event(*, stage: str, error: str, detail: str | None = None) -> str:
            payload: dict[str, Any] = {"run_id": run_id, "stage": stage, "error": error}
            if detail:
                payload["detail"] = detail
            return f"event: error\ndata: {json.dumps(payload)}\n\n"

        yield progress(
            {
                "stage": "starting",
                "message": "Initializing refresh stream...",
                "current": 0,
                "total": 0,
                "heartbeat": True,
                "elapsed_ms": 0,
            }
        )

        # 1. Get person
        try:
            person = await asyncio.to_thread(_get_person_details, db, person_id_str)
            if not person:
                yield error_event(stage="setup", error="Person not found")
                return

            external_ids = person.get("external_ids") or {}
            imdb_person_id = _extract_imdb_id(external_ids)
            tmdb_person_id = await asyncio.to_thread(_get_tmdb_id, db, person_id_str, external_ids)
            person_name = person.get("full_name")
            sources, fandom_skipped = await asyncio.to_thread(_resolve_refresh_sources, db, request)
            if fandom_skipped:
                errors.append("Fandom sources skipped for non-Real Housewives show context.")
        except Exception as exc:  # noqa: BLE001
            logger.exception("Refresh stream setup failed for %s: %s", person_id_str, exc)
            yield error_event(stage="setup", error="Failed to initialize refresh", detail=str(exc))
            return

        # 1.5 Refresh person profiles (best-effort)
        yield progress({"stage": "tmdb_profile", "message": "Syncing TMDb profile..."})
        tmdb_profile_status, tmdb_profile_error_code, tmdb_profile_error_detail = await asyncio.to_thread(
            _run_tmdb_profile_refresh,
            db,
            person_id_str,
            tmdb_person_id=tmdb_person_id,
        )
        if tmdb_profile_status == "failed":
            errors.append(f"TMDb profile [{tmdb_profile_error_code}]: {tmdb_profile_error_detail}")
            yield progress(
                {
                    "stage": "tmdb_profile",
                    "message": f"TMDb profile sync failed ({tmdb_profile_error_code}).",
                    "tmdb_profile_status": tmdb_profile_status,
                    "tmdb_profile_error_code": tmdb_profile_error_code,
                    "tmdb_profile_error_detail": tmdb_profile_error_detail,
                    "current": 1,
                    "total": 1,
                }
            )
        elif tmdb_profile_status == "skipped":
            yield progress(
                {
                    "stage": "tmdb_profile",
                    "message": "TMDb profile sync skipped.",
                    "tmdb_profile_status": tmdb_profile_status,
                    "tmdb_profile_error_code": tmdb_profile_error_code,
                    "tmdb_profile_error_detail": tmdb_profile_error_detail,
                    "current": 1,
                    "total": 1,
                }
            )
        else:
            yield progress(
                {
                    "stage": "tmdb_profile",
                    "message": "TMDb profile synced.",
                    "tmdb_profile_status": tmdb_profile_status,
                    "current": 1,
                    "total": 1,
                }
            )

        if "fandom" in sources or "fandom-gallery" in sources:
            yield progress({"stage": "fandom_profile", "message": "Syncing Fandom profile..."})
            try:
                await asyncio.to_thread(_refresh_fandom_profile, db, person_id_str, person_name=person_name)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Fandom profile: {exc}")
        else:
            yield progress(
                {
                    "stage": "fandom_profile",
                    "message": "Skipping Fandom profile (non-Real Housewives show context).",
                }
            )

        imdb_allowed_title_ids = set()
        imdb_allowed_keywords: list[str] = []
        imdb_prioritize_solo = False
        try:
            imdb_allowed_title_ids, imdb_allowed_keywords, imdb_prioritize_solo = await asyncio.to_thread(
                _resolve_imdb_focus_filters,
                db,
                request.show_id,
                request.show_name,
            )
        except Exception:  # noqa: BLE001
            imdb_allowed_title_ids = set()
            imdb_allowed_keywords = []
            imdb_prioritize_solo = False

        try:
            imdb_strict_context = await asyncio.to_thread(
                _resolve_imdb_traitors_strict_context,
                db,
                show_id=request.show_id,
                show_name=request.show_name,
                target_person_imdb_id=imdb_person_id,
                target_person_name=person_name,
            )
        except Exception:  # noqa: BLE001
            imdb_strict_context = {
                "strict_mode_enabled": False,
                "strict_types": set(),
                "target_person_imdb_id": imdb_person_id,
                "target_person_name": person_name,
                "allowed_cast_imdb_ids": set(),
                "allowed_cast_names": set(),
                "allowed_episode_imdb_ids": set(),
            }

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
        source_skip_details: list[dict[str, Any]] = []
        photos: list[dict[str, Any]] = []

        for stage, label in fetch_steps:
            source_name: SourceType | None = None
            if stage == "sync_imdb":
                source_name = "imdb"
            elif stage == "sync_tmdb":
                source_name = "tmdb"
            elif stage == "sync_fandom":
                source_name = "fandom"
            elif stage == "sync_fandom_gallery":
                source_name = "fandom-gallery"

            source_total = (
                await asyncio.to_thread(_get_known_source_total, source_name, imdb_person_id, tmdb_person_id)
                if source_name in {"imdb", "tmdb"}
                else None
            )
            mirrored_count = (
                await asyncio.to_thread(_count_mirrored_cast_photos, db, person_id_str, source_name)
                if source_name and source_total is not None
                else None
            )
            yield progress(
                {
                    "stage": stage,
                    "message": f"Syncing {label}...",
                    "current": processed_sources,
                    "total": total_sources,
                    "source": source_name,
                    "source_total": source_total,
                    "mirrored_count": mirrored_count,
                    "heartbeat": True,
                    "elapsed_ms": 0,
                    **(dict(imdb_diagnostics) if stage == "sync_imdb" else {}),
                }
            )
            rows: list[dict[str, Any]] = []
            stage_started_at = time.perf_counter()
            try:
                fetch_result: dict[str, Any] = {"rows": [], "error": None}
                stage_key = stage
                stage_person_id = person_id_str
                stage_imdb_person_id = imdb_person_id
                stage_tmdb_person_id = tmdb_person_id
                stage_person_name = str(person_name)
                stage_limit = request.limit_per_source
                stage_imdb_allowed_title_ids = imdb_allowed_title_ids
                stage_imdb_allowed_keywords = imdb_allowed_keywords
                stage_imdb_prioritize_solo = imdb_prioritize_solo
                stage_imdb_strict_context = imdb_strict_context

                def run_source_fetch(
                    *,
                    result: dict[str, Any] = fetch_result,
                    stage_name: str = stage_key,
                    person_identifier: str = stage_person_id,
                    imdb_identifier: str | None = stage_imdb_person_id,
                    tmdb_identifier: int | None = stage_tmdb_person_id,
                    name: str = stage_person_name,
                    limit: int = stage_limit,
                    allowed_title_imdb_ids: tuple[str, ...] = tuple(stage_imdb_allowed_title_ids),
                    allowed_title_keywords: tuple[str, ...] = tuple(stage_imdb_allowed_keywords),
                    prioritize_solo: bool = stage_imdb_prioritize_solo,
                    strict_context: dict[str, Any] = stage_imdb_strict_context,
                    diagnostics: dict[str, int] = imdb_diagnostics,
                ) -> None:
                    try:
                        if stage_name == "sync_imdb":
                            result["rows"] = fetch_imdb_cast_photos(
                                imdb_identifier,
                                person_identifier,
                                limit=limit,
                                allowed_title_imdb_ids=set(allowed_title_imdb_ids),
                                allowed_title_keywords=list(allowed_title_keywords),
                                prioritize_solo_people=prioritize_solo,
                                strict_types=set(strict_context.get("strict_types") or set()),
                                target_person_imdb_id=strict_context.get("target_person_imdb_id"),
                                target_person_name=strict_context.get("target_person_name"),
                                allowed_cast_imdb_ids=set(strict_context.get("allowed_cast_imdb_ids") or set()),
                                allowed_cast_names=set(strict_context.get("allowed_cast_names") or set()),
                                allowed_episode_imdb_ids=set(strict_context.get("allowed_episode_imdb_ids") or set()),
                                strict_mode_enabled=bool(strict_context.get("strict_mode_enabled")),
                                imdb_diagnostics=diagnostics,
                                session=None,
                                verbose=False,
                            )
                        elif stage_name == "sync_tmdb":
                            result["rows"] = fetch_tmdb_cast_photos(
                                int(tmdb_identifier),
                                person_identifier,
                                imdb_person_id=imdb_identifier,
                                limit=limit,
                                verbose=False,
                            )
                        elif stage_name == "sync_fandom":
                            result["rows"] = fetch_fandom_person_cast_photos(
                                name,
                                person_identifier,
                                imdb_person_id=imdb_identifier,
                                limit=limit,
                                verbose=False,
                            )
                        else:
                            result["rows"] = fetch_fandom_gallery_cast_photos(
                                name,
                                person_identifier,
                                imdb_person_id=imdb_identifier,
                                limit=limit,
                                verbose=False,
                            )
                    except Exception as exc:  # noqa: BLE001
                        result["error"] = exc

                fetch_task = asyncio.create_task(asyncio.to_thread(run_source_fetch))
                while not fetch_task.done():
                    await asyncio.sleep(2)
                    if fetch_task.done():
                        break
                    elapsed_ms = int((time.perf_counter() - stage_started_at) * 1000)
                    yield progress(
                        {
                            "stage": stage,
                            "message": f"Syncing {label}...",
                            "current": processed_sources,
                            "total": total_sources,
                            "source": source_name,
                            "source_total": source_total,
                            "mirrored_count": mirrored_count,
                            "heartbeat": True,
                            "elapsed_ms": elapsed_ms,
                            **(dict(imdb_diagnostics) if stage == "sync_imdb" else {}),
                        }
                    )
                await fetch_task
                if fetch_result["error"] is not None:
                    raise fetch_result["error"]
                rows = fetch_result["rows"] if isinstance(fetch_result["rows"], list) else []
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{label}: {exc}")
                rows = []
            photos.extend(rows)
            processed_sources += 1
            elapsed_ms = int((time.perf_counter() - stage_started_at) * 1000)
            yield progress(
                {
                    "stage": stage,
                    "message": f"Synced {label} ({len(rows)} photos).",
                    "current": processed_sources,
                    "total": total_sources,
                    "source": source_name,
                    "source_total": source_total,
                    "mirrored_count": mirrored_count,
                    "elapsed_ms": elapsed_ms,
                    **(dict(imdb_diagnostics) if stage == "sync_imdb" else {}),
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
            episode_metadata_tagged, episode_failed = await asyncio.to_thread(
                _enrich_cast_photos_with_episode_metadata, db, photos
            )
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
            show_context_tagged, show_failed = await asyncio.to_thread(
                _apply_show_context_to_photos,
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
                    upserted = await asyncio.to_thread(
                        upsert_cast_photos, db, imdb_photos, dedupe_on="source_image_id"
                    )
                    photos_upserted += len(upserted)
                    upserted_photo_ids.extend([str(row["id"]) for row in upserted if row.get("id")])
                if other_photos:
                    upserted = await asyncio.to_thread(
                        upsert_cast_photos, db, other_photos, dedupe_on="image_url_canonical"
                    )
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

        # 3.5 Repair existing IMDb rows for this person.
        yield progress(
            {
                "stage": "metadata_repair",
                "message": "Repairing existing IMDb metadata...",
                "current": 0,
                "total": 1,
            }
        )
        try:
            existing_imdb_rows_repaired, repair_failed = await asyncio.to_thread(
                _repair_existing_imdb_cast_photos,
                db,
                person_id_str,
                show_id=request.show_id,
                show_name=request.show_name,
                strict_context=imdb_strict_context,
            )
            metadata_enrichment_failed += repair_failed
            yield progress(
                {
                    "stage": "metadata_repair",
                    "message": f"Existing IMDb metadata repaired ({existing_imdb_rows_repaired} rows).",
                    "current": 1,
                    "total": 1,
                }
            )
        except Exception as exc:  # noqa: BLE001
            metadata_enrichment_failed += 1
            errors.append(f"Metadata repair: {exc}")
            yield progress(
                {
                    "stage": "metadata_repair",
                    "message": f"Existing IMDb metadata repair failed: {exc}",
                    "current": 1,
                    "total": 1,
                }
            )

        # 4. Mirror
        cast_photos_mirrored, cast_photos_failed = 0, 0
        media_assets_mirrored, media_assets_failed = 0, 0
        if not request.skip_mirror:
            try:
                yield progress(
                    {
                        "stage": "mirroring",
                        "message": "Mirroring cast photos...",
                        "current": 0,
                        "total": 2,
                    }
                )
                cast_photos_mirrored, cast_photos_failed = await asyncio.to_thread(
                    _mirror_person_photos,
                    db,
                    person_id_str,
                    imdb_person_id,
                    force=request.force_mirror,
                )
                yield progress(
                    {
                        "stage": "mirroring",
                        "message": (
                            f"Mirrored cast photos ({cast_photos_mirrored} hosted, {cast_photos_failed} failed)."
                        ),
                        "current": 1,
                        "total": 2,
                    }
                )

                yield progress(
                    {
                        "stage": "mirroring",
                        "message": "Mirroring media assets...",
                        "current": 1,
                        "total": 2,
                    }
                )
                media_assets_mirrored, media_assets_failed = await asyncio.to_thread(
                    _mirror_person_media_assets,
                    db,
                    person_id_str,
                    force=request.force_mirror,
                )
                yield progress(
                    {
                        "stage": "mirroring",
                        "message": (
                            f"Mirrored media assets ({media_assets_mirrored} hosted, {media_assets_failed} failed)."
                        ),
                        "current": 2,
                        "total": 2,
                    }
                )
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
        photos_mirrored = cast_photos_mirrored + media_assets_mirrored
        photos_failed = cast_photos_failed + media_assets_failed

        # 5. Prune
        photos_pruned = 0
        if not request.skip_mirror and not request.skip_prune:
            yield progress({"stage": "pruning", "message": "Pruning orphaned S3 objects..."})
            photos_pruned = await asyncio.to_thread(
                _prune_person_s3_objects, db, imdb_person_id or person_id_str
            )
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
        if request.skip_auto_count:
            yield progress(
                {
                    "stage": "auto_count",
                    "message": "Skipping auto-count (request).",
                    "current": 0,
                    "total": 0,
                }
            )
        else:
            try:
                from trr_backend.clients.screenalytics import (
                    ScreenalyticsClientError,
                    ScreenalyticsUnavailableError,
                    count_people,
                    get_screenalytics_unavailable_state,
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
                    unavailable, retry_after_s, unavailable_reason = get_screenalytics_unavailable_state()
                    if unavailable:
                        yield progress(
                            {
                                "stage": "auto_count",
                                "message": "Skipping auto-count (Screenalytics unavailable).",
                                "current": 0,
                                "total": 0,
                                "skip_reason": "service_unavailable",
                                "service_unavailable": True,
                                "retry_after_s": retry_after_s,
                                "detail": unavailable_reason,
                            }
                        )
                    else:
                        candidate_sources = [s for s in sources if s in ALL_SOURCES]
                        def _fetch_auto_count_candidates() -> tuple[list, dict, list]:
                            _cast = (
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
                            _tags = get_tags_by_photo_ids(db, [str(r["id"]) for r in _cast if r.get("id")])
                            _media = _fetch_person_media_link_rows(db, person_id_str)
                            return _cast, _tags, _media

                        cast_rows, tag_rows, media_rows = await asyncio.to_thread(_fetch_auto_count_candidates)

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
                                    "media_asset_id": str(row.get("media_asset_id") or ""),
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
                        service_unavailable_error: ScreenalyticsUnavailableError | None = None
                        for idx, entry in enumerate(to_process, start=1):
                            auto_counts_attempted += 1
                            result = None
                            selected_image_url: str | None = None
                            last_error: ScreenalyticsClientError | None = None
                            for url in entry["urls"]:
                                try:
                                    result = await asyncio.to_thread(count_people, url)
                                    selected_image_url = url
                                    break
                                except ScreenalyticsUnavailableError as exc:
                                    service_unavailable_error = exc
                                    last_error = exc
                                    break
                                except ScreenalyticsClientError as exc:
                                    last_error = exc
                            if service_unavailable_error is not None:
                                auto_counts_failed += 1
                                retry_after = max(int(service_unavailable_error.retry_after_s), 1)
                                detail = str(service_unavailable_error) or "Screenalytics unavailable"
                                errors.append(f"Auto-count service unavailable: {detail}")
                                yield progress(
                                    {
                                        "stage": "auto_count",
                                        "message": "Auto-count paused (Screenalytics unavailable).",
                                        "current": max(0, idx - 1),
                                        "total": total_to_count,
                                        "skip_reason": "service_unavailable",
                                        "service_unavailable": True,
                                        "retry_after_s": retry_after,
                                        "detail": detail,
                                    }
                                )
                                break
                            try:
                                if result is None:
                                    raise last_error or ScreenalyticsClientError("Unable to auto-count image")
                                face_boxes = _build_face_boxes(result)
                                auto_people_ids, auto_people_names = _auto_people_from_face_boxes(face_boxes)
                                if entry["origin"] == "cast_photos":
                                    tag_row = entry.get("tag_row")
                                    existing_people_names = tag_row.get("people_names") if tag_row else None
                                    existing_people_ids = tag_row.get("people_ids") if tag_row else None
                                    await asyncio.to_thread(
                                        upsert_cast_photo_tags,
                                        db,
                                        cast_photo_id=entry["id"],
                                        people_names=(
                                            existing_people_names
                                            if existing_people_names
                                            else (auto_people_names or None)
                                        ),
                                        people_ids=(
                                            existing_people_ids
                                            if existing_people_ids
                                            else (auto_people_ids or None)
                                        ),
                                        people_count=result.people_count,
                                        people_count_source="auto",
                                        detector=result.detector,
                                        updated_by_firebase_uid="system:auto",
                                    )
                                    metadata = dict(entry["row"].get("metadata") or {})
                                    metadata_changed = False
                                    face_crops: list[dict[str, Any]] = []
                                    if selected_image_url and face_boxes:
                                        face_crops = await asyncio.to_thread(
                                            generate_and_upload_face_crops,
                                            entity_kind="cast_photo",
                                            entity_id=entry["id"],
                                            image_url=selected_image_url,
                                            face_boxes=face_boxes,
                                            size=256,
                                        )
                                    if face_boxes:
                                        if metadata.get("face_boxes") != face_boxes:
                                            metadata["face_boxes"] = face_boxes
                                            metadata_changed = True
                                    elif "face_boxes" in metadata:
                                        metadata.pop("face_boxes", None)
                                        metadata_changed = True
                                    if face_crops:
                                        if metadata.get("face_crops") != face_crops:
                                            metadata["face_crops"] = face_crops
                                            metadata_changed = True
                                    elif "face_crops" in metadata:
                                        metadata.pop("face_crops", None)
                                        metadata_changed = True
                                    crop_payload = _apply_auto_crop_payload(result)
                                    if crop_payload is not None:
                                        if not _is_manual_thumbnail_crop(metadata.get("thumbnail_crop")):
                                            metadata["thumbnail_crop"] = crop_payload
                                            metadata_changed = True
                                    if metadata_changed:
                                        await asyncio.to_thread(
                                            lambda metadata_payload=metadata, entry_id=entry["id"]: db.schema("core")
                                            .table("cast_photos")
                                            .update({"metadata": metadata_payload})
                                            .eq("id", entry_id)
                                            .execute()
                                        )
                                else:
                                    context = dict(entry.get("context") or {})
                                    context["people_count"] = result.people_count
                                    context["people_count_source"] = "auto"
                                    context["people_count_detector"] = result.detector
                                    context["face_boxes"] = face_boxes
                                    face_crops = []
                                    if selected_image_url and face_boxes:
                                        media_asset_id = str(
                                            entry.get("media_asset_id") or entry.get("id") or ""
                                        ).strip()
                                        face_crops = await asyncio.to_thread(
                                            generate_and_upload_face_crops,
                                            entity_kind="media_asset",
                                            entity_id=media_asset_id,
                                            image_url=selected_image_url,
                                            face_boxes=face_boxes,
                                            size=256,
                                        )
                                    context["face_crops"] = face_crops
                                    if not (
                                        isinstance(context.get("people_ids"), list) and context.get("people_ids")
                                    ) and auto_people_ids:
                                        context["people_ids"] = auto_people_ids
                                    if not (
                                        isinstance(context.get("people_names"), list) and context.get("people_names")
                                    ) and auto_people_names:
                                        context["people_names"] = auto_people_names
                                    crop_payload = _apply_auto_crop_payload(result)
                                    if crop_payload is not None and not _is_manual_thumbnail_crop(
                                        context.get("thumbnail_crop")
                                    ):
                                        context["thumbnail_crop"] = crop_payload
                                    await asyncio.to_thread(
                                        lambda context_payload=context, entry_id=entry["id"]: db.schema("core")
                                        .table("media_links")
                                        .update(
                                            {
                                                "context": context_payload,
                                                "updated_at": datetime.now(UTC).isoformat(),
                                            }
                                        )
                                        .eq("id", entry_id)
                                        .execute()
                                    )
                                auto_counts_succeeded += 1
                            except Exception as exc:  # noqa: BLE001
                                auto_counts_failed += 1
                                errors.append(f"Auto-count {entry['id']}: {exc}")
                                continue

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
                            "skip_reason": "not_configured",
                        }
                    )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Auto-count: {exc}")

        # 5.6 Word ID / text overlay detection (best-effort)
        text_overlay_attempted = 0
        text_overlay_succeeded = 0
        text_overlay_unknown = 0
        text_overlay_failed = 0
        text_overlay_configured = False
        text_overlay_candidates = 0
        text_overlay_skipped_reason: str | None = None
        if request.skip_word_detection:
            text_overlay_skipped_reason = "request_skip"
            yield progress(
                {
                    "stage": "word_id",
                    "message": "Skipping word detection (request).",
                    "current": 0,
                    "total": 0,
                }
            )
        else:
            try:
                from trr_backend.vision.text_overlay import (
                    TextOverlayDetectionError,
                    classify_text_overlay_failure_reason,
                    detect_and_update_cast_photo_text_overlay,
                    detect_and_update_media_asset_text_overlay,
                    is_text_overlay_detection_configured,
                )

                text_overlay_configured = is_text_overlay_detection_configured()
                if text_overlay_configured:
                    def _fetch_text_overlay_candidates() -> tuple[list, list]:
                        _cast = (
                            db.schema("core")
                            .table("cast_photos")
                            .select("id, metadata, source")
                            .eq("person_id", person_id_str)
                            .in_("source", [s for s in sources if s in ALL_SOURCES])
                            .execute()
                            .data
                            or []
                        )
                        _media = _fetch_person_media_link_rows(db, person_id_str)
                        return _cast, _media

                    cast_rows, media_rows = await asyncio.to_thread(_fetch_text_overlay_candidates)

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
                    text_overlay_candidates = total_text
                    if total_text == 0:
                        text_overlay_skipped_reason = "no_pending_images"
                        yield progress(
                            {
                                "stage": "word_id",
                                "message": "Text overlay already up to date (no pending images).",
                                "current": 0,
                                "total": 0,
                            }
                        )
                    else:
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
                                    result = await asyncio.to_thread(
                                        detect_and_update_cast_photo_text_overlay, db, item["id"], force=False
                                    )
                                else:
                                    result = await asyncio.to_thread(
                                        detect_and_update_media_asset_text_overlay, db, item["id"], force=False
                                    )
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
                    text_overlay_skipped_reason = "not_configured"
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
        if request.skip_centering:
            yield progress(
                {
                    "stage": "centering_cropping",
                    "message": "Skipping centering/cropping (request).",
                    "current": 0,
                    "total": 0,
                }
            )
        else:
            try:
                from trr_backend.clients.screenalytics import (
                    ScreenalyticsClientError,
                    ScreenalyticsUnavailableError,
                    count_people,
                    get_screenalytics_unavailable_state,
                    is_screenalytics_configured,
                )

                if is_screenalytics_configured():
                    unavailable, retry_after_s, unavailable_reason = get_screenalytics_unavailable_state()
                    if unavailable:
                        yield progress(
                            {
                                "stage": "centering_cropping",
                                "message": "Skipping centering/cropping (Screenalytics unavailable).",
                                "current": 0,
                                "total": 0,
                                "skip_reason": "service_unavailable",
                                "service_unavailable": True,
                                "retry_after_s": retry_after_s,
                                "detail": unavailable_reason,
                            }
                        )
                    else:
                        candidate_sources = [s for s in sources if s in ALL_SOURCES]

                        def _fetch_centering_candidates() -> tuple[list, list]:
                            _cast = (
                                db.schema("core")
                                .table("cast_photos")
                                .select(
                                    "id, hosted_url, url, image_url, thumb_url, "
                                    "source_page_url, source, metadata"
                                )
                                .eq("person_id", person_id_str)
                                .in_("source", candidate_sources)
                                .execute()
                                .data
                                or []
                            )
                            _media = _fetch_person_media_link_rows(db, person_id_str)
                            return _cast, _media

                        cast_rows, media_rows = await asyncio.to_thread(_fetch_centering_candidates)

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
                        service_unavailable_error: ScreenalyticsUnavailableError | None = None
                        for idx, entry in enumerate(to_process_crop, start=1):
                            centering_attempted += 1
                            result = None
                            last_error: ScreenalyticsClientError | None = None
                            for url in entry["urls"]:
                                try:
                                    result = await asyncio.to_thread(count_people, url)
                                    break
                                except ScreenalyticsUnavailableError as exc:
                                    service_unavailable_error = exc
                                    last_error = exc
                                    break
                                except ScreenalyticsClientError as exc:
                                    last_error = exc
                            if service_unavailable_error is not None:
                                centering_failed += 1
                                retry_after = max(int(service_unavailable_error.retry_after_s), 1)
                                detail = str(service_unavailable_error) or "Screenalytics unavailable"
                                errors.append(f"Centering service unavailable: {detail}")
                                yield progress(
                                    {
                                        "stage": "centering_cropping",
                                        "message": "Centering/cropping paused (Screenalytics unavailable).",
                                        "current": max(0, idx - 1),
                                        "total": total_crop,
                                        "skip_reason": "service_unavailable",
                                        "service_unavailable": True,
                                        "retry_after_s": retry_after,
                                        "detail": detail,
                                    }
                                )
                                break
                            try:
                                if result is None:
                                    raise last_error or ScreenalyticsClientError("Unable to center/crop image")
                                crop_payload = _apply_auto_crop_payload(result)
                                if crop_payload is None:
                                    raise ScreenalyticsClientError("No detections available for centering/cropping")
                                if entry["origin"] == "cast_photos":
                                    metadata = dict(entry["metadata"] or {})
                                    metadata["thumbnail_crop"] = crop_payload
                                    await asyncio.to_thread(
                                        lambda metadata_payload=metadata, entry_id=entry["id"]: db.schema("core")
                                        .table("cast_photos")
                                        .update({"metadata": metadata_payload})
                                        .eq("id", entry_id)
                                        .execute()
                                    )
                                else:
                                    context = dict(entry["context"] or {})
                                    context["thumbnail_crop"] = crop_payload
                                    await asyncio.to_thread(
                                        lambda context_payload=context, entry_id=entry["id"]: db.schema("core")
                                        .table("media_links")
                                        .update(
                                            {
                                                "context": context_payload,
                                                "updated_at": datetime.now(UTC).isoformat(),
                                            }
                                        )
                                        .eq("id", entry_id)
                                        .execute()
                                    )
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
                            "skip_reason": "not_configured",
                        }
                    )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Centering/Cropping: {exc}")

        # 5.8 Resize/variant generation stage (best-effort for cast + media)
        resize_attempted = 0
        resize_succeeded = 0
        resize_failed = 0
        resize_crop_attempted = 0
        resize_crop_succeeded = 0
        resize_crop_failed = 0
        if request.skip_resize:
            yield progress(
                {
                    "stage": "resizing",
                    "message": "Skipping resize/variant generation (request).",
                    "current": 0,
                    "total": 0,
                }
            )
        else:
            try:
                resize_started_at = time.perf_counter()
                resize_progress_lock = Lock()
                resize_progress_current = 0
                resize_progress_total = 1

                def _update_resize_progress(current: int, total: int) -> None:
                    nonlocal resize_progress_current, resize_progress_total
                    next_current = max(0, int(current))
                    next_total = max(1, int(total))
                    with resize_progress_lock:
                        resize_progress_current = next_current
                        resize_progress_total = next_total

                yield progress(
                    {
                        "stage": "resizing",
                        "message": "Generating resized variants...",
                        "current": 0,
                        "total": 1,
                    }
                )
                resize_task = asyncio.create_task(
                    asyncio.to_thread(
                        _resize_person_gallery_images,
                        db,
                        person_id_str,
                        sources,
                        force=False,
                        progress_cb=_update_resize_progress,
                    )
                )
                while not resize_task.done():
                    await asyncio.sleep(2)
                    if resize_task.done():
                        break
                    with resize_progress_lock:
                        progress_current = resize_progress_current
                        progress_total = resize_progress_total
                    yield progress(
                        {
                            "stage": "resizing",
                            "message": "Generating resized variants...",
                            "current": progress_current,
                            "total": progress_total,
                            "heartbeat": True,
                            "elapsed_ms": int((time.perf_counter() - resize_started_at) * 1000),
                        }
                    )
                (
                    resize_attempted,
                    resize_succeeded,
                    resize_failed,
                    resize_crop_attempted,
                    resize_crop_succeeded,
                    resize_crop_failed,
                ) = await resize_task
                resize_total_ops = max(0, resize_attempted + resize_crop_attempted)
                resize_processed_ops = max(
                    0,
                    resize_succeeded + resize_failed + resize_crop_succeeded + resize_crop_failed,
                )
                yield progress(
                    {
                        "stage": "resizing",
                        "message": (
                            "Variant generation complete "
                            f"({resize_succeeded}/{resize_attempted} base, "
                            f"{resize_crop_succeeded}/{resize_crop_attempted} crop)."
                        ),
                        "current": min(resize_processed_ops, resize_total_ops),
                        "total": resize_total_ops,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Resizing: {exc}")

        # 6. Complete
        complete_data = {
            "run_id": run_id,
            "person_id": person_id_str,
            "photos_fetched": len(photos),
            "photos_upserted": photos_upserted,
            "tmdb_profile_status": tmdb_profile_status,
            "tmdb_profile_error_code": tmdb_profile_error_code,
            "tmdb_profile_error_detail": tmdb_profile_error_detail,
            "photos_mirrored": photos_mirrored,
            "photos_failed": photos_failed,
            "cast_photos_mirrored": cast_photos_mirrored,
            "cast_photos_failed": cast_photos_failed,
            "media_assets_mirrored": media_assets_mirrored,
            "media_assets_failed": media_assets_failed,
            "photos_pruned": photos_pruned,
            "imdb_pages_scanned": int(imdb_diagnostics.get("imdb_pages_scanned", 0)),
            "imdb_candidates_seen": int(imdb_diagnostics.get("imdb_candidates_seen", 0)),
            "imdb_kept": int(imdb_diagnostics.get("imdb_kept", 0)),
            "imdb_filtered_type": int(imdb_diagnostics.get("imdb_filtered_type", 0)),
            "imdb_filtered_people": int(imdb_diagnostics.get("imdb_filtered_people", 0)),
            "imdb_filtered_episode": int(imdb_diagnostics.get("imdb_filtered_episode", 0)),
            "imdb_filtered_other": int(imdb_diagnostics.get("imdb_filtered_other", 0)),
            "episode_metadata_tagged": episode_metadata_tagged,
            "show_context_tagged": show_context_tagged,
            "metadata_enrichment_failed": metadata_enrichment_failed,
            "existing_imdb_rows_repaired": existing_imdb_rows_repaired,
            "auto_counts_attempted": auto_counts_attempted,
            "auto_counts_succeeded": auto_counts_succeeded,
            "auto_counts_failed": auto_counts_failed,
            "text_overlay_attempted": text_overlay_attempted,
            "text_overlay_succeeded": text_overlay_succeeded,
            "text_overlay_unknown": text_overlay_unknown,
            "text_overlay_failed": text_overlay_failed,
            "text_overlay_failure_reasons": text_overlay_reason_counts,
            "text_overlay_configured": text_overlay_configured,
            "text_overlay_candidates": text_overlay_candidates,
            "text_overlay_skipped_reason": text_overlay_skipped_reason,
            "centering_attempted": centering_attempted,
            "centering_succeeded": centering_succeeded,
            "centering_failed": centering_failed,
            "centering_skipped_manual": centering_skipped_manual,
            "resize_attempted": resize_attempted,
            "resize_succeeded": resize_succeeded,
            "resize_failed": resize_failed,
            "resize_crop_attempted": resize_crop_attempted,
            "resize_crop_succeeded": resize_crop_succeeded,
            "resize_crop_failed": resize_crop_failed,
            "sources_skipped": len(source_skip_details),
            "source_skip_details": source_skip_details,
            "live_counts": build_live_counts(),
            "errors": errors,
        }
        yield f"event: complete\ndata: {json.dumps(complete_data)}\n\n"

    async def guarded_event_generator() -> AsyncGenerator[str, None]:
        try:
            async for chunk in event_generator():
                yield chunk
        except Exception as exc:  # noqa: BLE001
            logger.exception("Refresh stream runtime failure for %s: %s", person_id_str, exc)
            payload = {
                "run_id": run_id,
                "stage": "stream",
                "error": "Refresh stream failed",
                "detail": str(exc),
            }
            yield f"event: error\ndata: {json.dumps(payload)}\n\n"

    return StreamingResponse(
        guarded_event_generator(),
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
    request: ReprocessImagesRequest = Body(default_factory=ReprocessImagesRequest),
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> StreamingResponse:
    """Re-run counting, text-ID, centering, and resize on existing photos (no sync/mirror)."""
    person_id_str = str(person_id)
    run_id = f"reprocess-{person_id_str}-{int(datetime.now(UTC).timestamp())}"

    async def event_generator() -> AsyncGenerator[str, None]:
        errors: list[str] = []
        text_overlay_reason_counts: dict[str, int] = dict.fromkeys(TEXT_OVERLAY_FAILURE_REASONS, 0)
        auto_counts_attempted = 0
        auto_counts_failed = 0
        auto_counts_succeeded = 0
        c_attempted = 0
        text_overlay_succeeded = 0
        c_succeeded = 0
        c_failed = 0
        c_skipped = 0
        resize_attempted = 0
        resize_succeeded = 0
        resize_failed = 0
        resize_crop_attempted = 0
        resize_crop_succeeded = 0
        resize_crop_failed = 0

        def build_live_counts() -> dict[str, int]:
            return {
                "synced": 0,
                "mirrored": 0,
                "counted": int(auto_counts_succeeded),
                "cropped": int(c_succeeded),
                "id_text": int(text_overlay_succeeded),
                "resized": int(resize_succeeded),
            }

        def progress(payload: dict[str, Any]) -> str:
            return (
                "event: progress\ndata: "
                + json.dumps({"run_id": run_id, "live_counts": build_live_counts(), **payload})
                + "\n\n"
            )

        def error_event(*, stage: str, error: str, detail: str | None = None) -> str:
            payload: dict[str, Any] = {"run_id": run_id, "stage": stage, "error": error}
            if detail:
                payload["detail"] = detail
            return f"event: error\ndata: {json.dumps(payload)}\n\n"

        # Verify person exists
        person = await asyncio.to_thread(_get_person_details, db, person_id_str)
        if not person:
            yield error_event(stage="setup", error="Person not found")
            return

        sources: list[SourceType] = list(request.sources or ALL_SOURCES)

        # ---------- Auto-count (cast_photos + media_links) ----------
        if request.run_count:
            yield progress(
                {
                    "stage": "auto_count",
                    "message": "Auto-counting people in images...",
                    "current": None,
                    "total": None,
                }
            )

            ac_cast, sc_cast, fc_cast = await asyncio.to_thread(
                _auto_count_cast_photos,
                db,
                person_id_str,
                sources,
                force_recount=True,
            )
            ac_media, sc_media, fc_media = await asyncio.to_thread(
                _auto_count_media_links,
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
        else:
            yield progress(
                {
                    "stage": "auto_count",
                    "message": "Skipping auto-count stage.",
                    "current": 0,
                    "total": 0,
                }
            )

        # ---------- Word ID / text overlay (cast_photos + media_links) ----------
        text_overlay_attempted = 0
        text_overlay_succeeded = 0
        text_overlay_unknown = 0
        text_overlay_failed = 0
        text_overlay_configured = False
        text_overlay_candidates = 0
        text_overlay_skipped_reason: str | None = None

        cast_candidate_ids: list[str] = []
        media_candidate_ids: list[str] = []

        if request.run_id_text:
            try:
                from trr_backend.vision.text_overlay import is_text_overlay_detection_configured

                text_overlay_configured = is_text_overlay_detection_configured()
            except Exception:
                text_overlay_configured = False
        else:
            text_overlay_skipped_reason = "stage_disabled"
            yield progress(
                {
                    "stage": "word_id",
                    "message": "Skipping word detection stage.",
                    "current": 0,
                    "total": 0,
                }
            )

        if request.run_id_text and text_overlay_configured:
            try:
                cast_rows = await asyncio.to_thread(
                    lambda: (
                        db.schema("core")
                        .table("cast_photos")
                        .select("id, metadata, source")
                        .eq("person_id", person_id_str)
                        .in_("source", [s for s in sources if s in ALL_SOURCES])
                        .execute()
                        .data
                        or []
                    )
                )
                for row in cast_rows:
                    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
                    if "has_text_overlay" in metadata:
                        continue
                    row_id = row.get("id")
                    if row_id:
                        cast_candidate_ids.append(str(row_id))
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Word ID candidate cast lookup: {exc}")
    
            try:
                media_rows = await asyncio.to_thread(_fetch_person_media_link_rows, db, person_id_str)
                seen_asset_ids: set[str] = set()
                for row in media_rows:
                    asset_id = str(row.get("media_asset_id") or "")
                    if not asset_id or asset_id in seen_asset_ids:
                        continue
                    seen_asset_ids.add(asset_id)
                    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
                    if "has_text_overlay" in metadata:
                        continue
                    media_candidate_ids.append(asset_id)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Word ID candidate media lookup: {exc}")
    
            text_overlay_candidates = len(cast_candidate_ids) + len(media_candidate_ids)
    
            if text_overlay_candidates == 0:
                text_overlay_skipped_reason = "no_pending_images"
                yield progress(
                    {
                        "stage": "word_id",
                        "message": "Text overlay already up to date (no pending images).",
                        "current": 0,
                        "total": 0,
                    }
                )
            else:
                yield progress(
                    {
                        "stage": "word_id",
                        "message": "Detecting words/text overlays...",
                        "current": 0,
                        "total": text_overlay_candidates,
                    }
                )
    
                to_cast, ts_cast, tu_cast, tf_cast = await asyncio.to_thread(
                    _detect_text_overlay_cast_photos,
                    db,
                    person_id_str,
                    sources,
                    photo_ids=cast_candidate_ids,
                    reason_counts=text_overlay_reason_counts,
                )
                to_media, ts_media, tu_media, tf_media = await asyncio.to_thread(
                    _detect_text_overlay_media_links,
                    db,
                    person_id_str,
                    asset_ids=media_candidate_ids,
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
                        "total": text_overlay_candidates,
                    }
                )
        elif request.run_id_text:
            text_overlay_skipped_reason = "not_configured"
            yield progress(
                {
                    "stage": "word_id",
                    "message": "Skipping word detection (not configured).",
                    "current": 0,
                    "total": 0,
                }
            )

        # ---------- Centering / cropping ----------
        if request.run_crop:
            yield progress(
                {
                    "stage": "centering_cropping",
                    "message": "Centering/cropping thumbnails...",
                    "current": None,
                    "total": None,
                }
            )

            c_attempted, c_succeeded, c_failed, c_skipped = await asyncio.to_thread(
                _recenter_person_gallery_images,
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
        else:
            yield progress(
                {
                    "stage": "centering_cropping",
                    "message": "Skipping centering/cropping stage.",
                    "current": 0,
                    "total": 0,
                }
            )

        # ---------- Resize / variants ----------
        if request.run_resize:
            resize_started_at = time.perf_counter()
            resize_progress_lock = Lock()
            resize_progress_current = 0
            resize_progress_total = 1

            def _update_resize_progress(current: int, total: int) -> None:
                nonlocal resize_progress_current, resize_progress_total
                next_current = max(0, int(current))
                next_total = max(1, int(total))
                with resize_progress_lock:
                    resize_progress_current = next_current
                    resize_progress_total = next_total

            yield progress(
                {
                    "stage": "resizing",
                    "message": "Generating resized variants...",
                    "current": 0,
                    "total": 1,
                }
            )
            resize_task = asyncio.create_task(
                asyncio.to_thread(
                    _resize_person_gallery_images,
                    db,
                    person_id_str,
                    sources,
                    force=True,
                    progress_cb=_update_resize_progress,
                )
            )
            while not resize_task.done():
                await asyncio.sleep(2)
                if resize_task.done():
                    break
                with resize_progress_lock:
                    progress_current = resize_progress_current
                    progress_total = resize_progress_total
                yield progress(
                    {
                        "stage": "resizing",
                        "message": "Generating resized variants...",
                        "current": progress_current,
                        "total": progress_total,
                        "heartbeat": True,
                        "elapsed_ms": int((time.perf_counter() - resize_started_at) * 1000),
                    }
                )
            (
                resize_attempted,
                resize_succeeded,
                resize_failed,
                resize_crop_attempted,
                resize_crop_succeeded,
                resize_crop_failed,
            ) = await resize_task
            resize_total_ops = max(0, resize_attempted + resize_crop_attempted)
            resize_processed_ops = max(
                0,
                resize_succeeded + resize_failed + resize_crop_succeeded + resize_crop_failed,
            )
            yield progress(
                {
                    "stage": "resizing",
                    "message": (
                        "Variant generation complete "
                        f"({resize_succeeded}/{resize_attempted} base, "
                        f"{resize_crop_succeeded}/{resize_crop_attempted} crop)."
                    ),
                    "current": min(resize_processed_ops, resize_total_ops),
                    "total": resize_total_ops,
                }
            )
        else:
            yield progress(
                {
                    "stage": "resizing",
                    "message": "Skipping resize stage.",
                    "current": 0,
                    "total": 0,
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
            "text_overlay_configured": text_overlay_configured,
            "text_overlay_candidates": text_overlay_candidates,
            "text_overlay_skipped_reason": text_overlay_skipped_reason,
            "centering_attempted": c_attempted,
            "centering_succeeded": c_succeeded,
            "centering_failed": c_failed,
            "centering_skipped_manual": c_skipped,
            "resize_attempted": resize_attempted,
            "resize_succeeded": resize_succeeded,
            "resize_failed": resize_failed,
            "resize_crop_attempted": resize_crop_attempted,
            "resize_crop_succeeded": resize_crop_succeeded,
            "resize_crop_failed": resize_crop_failed,
            "live_counts": build_live_counts(),
            "errors": errors,
        }
        yield f"event: complete\ndata: {json.dumps(complete_data)}\n\n"

    async def guarded_event_generator() -> AsyncGenerator[str, None]:
        try:
            async for chunk in event_generator():
                yield chunk
        except Exception as exc:  # noqa: BLE001
            logger.exception("Reprocess stream runtime failure for %s: %s", person_id_str, exc)
            payload = {
                "run_id": run_id,
                "stage": "stream",
                "error": "Reprocess stream failed",
                "detail": str(exc),
            }
            yield f"event: error\ndata: {json.dumps(payload)}\n\n"

    return StreamingResponse(
        guarded_event_generator(),
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
