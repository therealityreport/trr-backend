"""
Admin endpoints for person image management.

Provides endpoints to:
1. Refresh images from sources (IMDb, TMDb, Fandom)
2. Mirror images to S3
3. Stream progress via SSE
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
import unicodedata
from collections import Counter
from collections.abc import AsyncGenerator, Callable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from threading import Lock
from typing import Any, Literal, TypeVar, cast
from urllib.parse import unquote, urlparse, urlunparse
from uuid import UUID

from fastapi import APIRouter, Body, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from api.auth import AdminUser, FacebankSeedAdminUser
from api.deps import SupabaseAdminClient
from trr_backend.media.face_crops import generate_and_upload_face_crops
from trr_backend.pipeline.admin_operations import operation_stream_response, start_operation_for_stream
from trr_backend.repositories.identity_assignment import (
    build_identity_candidate_person_ids as build_identity_candidate_person_ids_shared,
)
from trr_backend.repositories.identity_assignment import (
    is_trr_show_eligible as is_trr_show_eligible_shared,
)
from trr_backend.repositories.media_links import update_media_link_facebank_seed
from trr_backend.repositories.tagging_references import (
    build_owner_tagging_reference_profile,
    sync_owner_tagging_reference_usage,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/person", tags=["admin-person"])
TChunk = TypeVar("TChunk")
_GETTY_SOURCE_ID = "getty"
_GETTY_PERSON_GALLERY_VARIANT = "person_gallery_nbcumv_crosswalk"

# Valid sources for person images
SourceType = Literal["imdb", "tmdb", "fandom", "fandom-gallery", "nbcumv"]
ALL_SOURCES: list[SourceType] = ["imdb", "tmdb", "fandom", "fandom-gallery", "nbcumv"]
SourceProgressStatus = Literal["pending", "running", "completed", "skipped", "failed"]
SOURCE_PROGRESS_KEY_ORDER = ("imdb", "tmdb", "fandom", "fandom_gallery", "getty_nbcumv")
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
AUTO_COUNT_DIAGNOSTIC_FIELDS = (
    "auto_faces_detected",
    "auto_face_crops_generated",
    "auto_person_fallback_crops_generated",
    "auto_no_face_rows",
    "auto_identity_skipped_non_trr_show",
    "auto_detect_success_rows",
    "auto_detect_failed_rows",
    "auto_persist_success_rows",
    "auto_persist_failed_rows",
    "auto_crop_cache_success_rows",
    "auto_crop_cache_failed_rows",
)
IMDB_VIEWER_ID_RE = re.compile(r"/mediaviewer/(rm\d+)/", re.IGNORECASE)
IMDB_TITLE_ID_RE = re.compile(r"/title/(tt\d+)/", re.IGNORECASE)
OWNER_FACE_MATCH_SIMILARITY_MIN_DEFAULT = 0.50
FACE_MATCH_CROSS_FACE_LEAD_MIN = 0.45
FACE_MATCH_CROSS_FACE_LEAD_MIN_SIMILARITY = 0.30
FACE_MATCH_SCORE_EVIDENCE_MIN = 1e-6
IMDB_CREDIT_MEDIA_TYPE_BY_TITLE_TYPE: dict[str, str] = {
    "MOVIE": "Movie",
    "TVMOVIE": "TV Movie",
    "TVSPECIAL": "TV Special",
    "TVEPISODE": "TV Episode",
    "TVSERIES": "TV Series",
    "TVMINISERIES": "TV Mini Series",
    "TVSHORT": "TV Short",
    "SHORT": "Short",
    "VIDEO": "Video",
    "DOCUMENTARY": "Documentary",
}


def _is_internal_raw_stream_request(request: Request | Any | None) -> bool:
    headers = getattr(request, "headers", None)
    if headers is None:
        return False
    try:
        value = headers.get("x-trr-internal-raw-stream")
    except Exception:  # noqa: BLE001
        return False
    return str(value or "").strip() == "1"


class RefreshImagesRequest(BaseModel):
    """Request to refresh person images from sources."""

    sources: list[SourceType] | None = Field(
        default=None,
        description="Sources to fetch from. Default: all",
    )
    limit_per_source: int = Field(
        default=10_000,
        ge=1,
        description="Max images per source (default: 10000, effectively unlimited)",
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
    execution_profile: Literal["speed", "balanced", "safe"] = Field(
        default="speed",
        description="Execution profile for throughput tuning.",
    )
    max_parallelism: dict[str, int] | None = Field(
        default=None,
        description=(
            "Optional per-stage parallelism overrides, keys: sync|mirror|tagging|crop. "
            "Values must be positive integers."
        ),
    )
    batch_size: dict[str, int] | None = Field(
        default=None,
        description="Optional per-stage batch-size overrides, keys: tagging|mirror|crop.",
    )
    prefer_fast_pass: bool = Field(
        default=True,
        description="Prefer fast-pass detection path with guarded fallback in screenalytics.",
    )
    async_job: bool = Field(
        default=True,
        description=(
            "Compatibility switch for queue-backed execution. Current endpoint runs inline and ignores this flag."
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
    nbcumv_photos_fetched: int = 0
    nbcumv_assets_imported: int = 0
    nbcumv_assets_skipped: int = 0
    nbcumv_gallery_links_created: int = 0
    nbcumv_failed: int = 0
    getty_candidates_total: int = 0
    getty_matched_total: int = 0
    getty_unmatched_total: int = 0
    shared_nbcumv_total: int = 0
    shared_nbcumv_imported: int = 0
    nbcumv_only_total: int = 0
    nbcumv_only_imported: int = 0
    getty_only_imported: int = 0
    getty_snapshot_saved: bool = False
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
    auto_count_attempted_rows: int = 0
    auto_count_skipped_existing_rows: int = 0
    auto_count_retry_attempted_rows: int = 0
    auto_count_retry_succeeded_rows: int = 0
    auto_faces_detected: int = 0
    auto_face_crops_generated: int = 0
    auto_person_fallback_crops_generated: int = 0
    auto_no_face_rows: int = 0
    auto_identity_skipped_non_trr_show: int = 0
    auto_detect_success_rows: int = 0
    auto_detect_failed_rows: int = 0
    auto_persist_success_rows: int = 0
    auto_persist_failed_rows: int = 0
    auto_crop_cache_success_rows: int = 0
    auto_crop_cache_failed_rows: int = 0
    row_error_counts: dict[str, int] = Field(default_factory=dict)
    text_overlay_attempted: int = 0
    text_overlay_succeeded: int = 0
    text_overlay_unknown: int = 0
    text_overlay_failed: int = 0
    text_overlay_attempted_rows: int = 0
    text_overlay_skipped_existing_rows: int = 0
    text_overlay_retry_attempted_rows: int = 0
    text_overlay_retry_succeeded_rows: int = 0
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
    retry_attempts: dict[str, int] = Field(default_factory=dict)
    failed_parts: list[dict[str, Any]] = Field(default_factory=list)
    source_progress: dict[str, dict[str, Any]] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)


class ReprocessImagesRequest(BaseModel):
    """Request to reprocess existing gallery assets."""

    run_metadata: bool = Field(default=False, description="Run IMDb metadata repair stage.")
    run_count: bool = Field(
        default=True,
        description="Run tagging stage (compatibility alias for run_tagging).",
    )
    run_tagging: bool | None = Field(
        default=None,
        description=(
            "Run tagging stage (face boxes + identity matching + owner thumbnail focus). "
            "When omitted, run_count is used."
        ),
    )
    force_tagging_recount: bool = Field(
        default=False,
        description=(
            "Compatibility field. Reprocess tagging always runs as full-fix and reprocesses existing counted rows."
        ),
    )
    run_id_text: bool = Field(default=True, description="Run text overlay detection stage.")
    run_crop: bool = Field(default=True, description="Run centering/cropping stage.")
    run_resize: bool = Field(default=True, description="Run resize/variant generation stage.")
    sources: list[SourceType] | None = Field(
        default=None,
        description="Optional source filter for cast-photo stages.",
    )
    show_id: UUID | None = Field(
        default=None,
        description="Optional show context used for metadata repair and TRR-show eligibility checks.",
    )
    show_name: str | None = Field(
        default=None,
        description="Optional show name used for metadata repair and TRR-show eligibility checks.",
    )
    target_cast_photo_ids: list[str] | None = Field(
        default=None,
        description=(
            "Optional scoped cast_photos ids for reprocess stages. When omitted, all eligible cast rows are considered."
        ),
    )
    target_media_link_ids: list[str] | None = Field(
        default=None,
        description=(
            "Optional scoped media_links ids for reprocess stages. "
            "When omitted, all eligible media-link rows are considered."
        ),
    )
    execution_profile: Literal["speed", "balanced", "safe"] = Field(
        default="speed",
        description="Execution profile for reprocess throughput tuning.",
    )
    max_parallelism: dict[str, int] | None = Field(
        default=None,
        description=(
            "Optional per-stage parallelism overrides, keys: sync|mirror|tagging|crop. Values are positive integers."
        ),
    )
    batch_size: dict[str, int] | None = Field(
        default=None,
        description="Optional per-stage batch-size overrides, keys: tagging|mirror|crop.",
    )
    prefer_fast_pass: bool = Field(
        default=True,
        description="Prefer fast-pass detection path with guarded fallback in screenalytics.",
    )
    async_job: bool = Field(
        default=True,
        description=(
            "Compatibility switch for queue-backed execution. "
            "Current stream endpoint keeps in-request orchestration and ignores this flag."
        ),
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


def _load_person_credit_show_names(
    db: SupabaseAdminClient,
    person_id: str,
) -> list[str]:
    normalized_person_id = str(person_id or "").strip()
    if not normalized_person_id:
        return []

    try:
        response = (
            db.schema("core")
            .table("credits")
            .select("show_id,billing_order")
            .eq("person_id", normalized_person_id)
            .limit(5000)
            .execute()
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("Person credit show lookup failed person_id=%s error=%s", normalized_person_id, exc)
        return []
    if hasattr(response, "error") and response.error:
        logger.debug("Person credit show lookup error person_id=%s error=%s", normalized_person_id, response.error)
        return []

    rows = response.data if isinstance(getattr(response, "data", None), list) else []
    if not rows:
        return []

    ranked_show_ids: dict[str, tuple[int, int]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        show_id = str(row.get("show_id") or "").strip()
        if not show_id:
            continue
        billing_order = row.get("billing_order")
        if isinstance(billing_order, int):
            billing_rank = billing_order
        elif isinstance(billing_order, str) and billing_order.strip().isdigit():
            billing_rank = int(billing_order.strip())
        else:
            billing_rank = 10_000
        next_rank = (billing_rank, index)
        current_rank = ranked_show_ids.get(show_id)
        if current_rank is None or next_rank < current_rank:
            ranked_show_ids[show_id] = next_rank
    if not ranked_show_ids:
        return []

    ordered_show_ids = sorted(ranked_show_ids, key=lambda sid: ranked_show_ids.get(sid, (10_000, 10_000)))
    show_name_by_id: dict[str, str] = {}
    for chunk in _chunked(ordered_show_ids, 200):
        try:
            shows_response = db.schema("core").table("shows").select("id,name").in_("id", chunk).execute()
        except Exception as exc:  # noqa: BLE001
            logger.debug("Credited show names lookup failed person_id=%s error=%s", normalized_person_id, exc)
            return []
        if hasattr(shows_response, "error") and shows_response.error:
            logger.debug(
                "Credited show names lookup error person_id=%s error=%s",
                normalized_person_id,
                shows_response.error,
            )
            return []
        for show_row in shows_response.data or []:
            if not isinstance(show_row, dict):
                continue
            show_id = str(show_row.get("id") or "").strip()
            show_name = str(show_row.get("name") or "").strip()
            if show_id and show_name:
                show_name_by_id[show_id] = show_name

    ordered_names = [
        show_name_by_id[show_id]
        for show_id in ordered_show_ids
        if show_id in show_name_by_id and show_name_by_id[show_id]
    ]
    if not ordered_names:
        return []

    deduped_names: list[str] = []
    seen_names: set[str] = set()
    for show_name in ordered_names:
        normalized_name = show_name.strip().lower()
        if not normalized_name or normalized_name in seen_names:
            continue
        seen_names.add(normalized_name)
        deduped_names.append(show_name)

    primary_names = [show_name for show_name in deduped_names if not _is_wwhl_show_name(show_name)]
    wwhl_names = [show_name for show_name in deduped_names if _is_wwhl_show_name(show_name)]
    return primary_names + wwhl_names


def _normalize_scope_ids(values: list[str] | None) -> list[str] | None:
    if values is None:
        return None
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        cleaned = str(value).strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)
    return normalized


def _read_positive_int_env(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return max(1, default)
    try:
        parsed = int(raw)
    except ValueError:
        return max(1, default)
    return max(1, parsed)


def _normalize_source_progress_key(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower().replace("-", "_")
    if not normalized:
        return None
    if normalized == "nbcumv":
        return "getty_nbcumv"
    return normalized


def _empty_source_progress_entry() -> dict[str, Any]:
    return {
        "discovered_total": None,
        "scraped_current": 0,
        "saved_current": 0,
        "failed_current": 0,
        "skipped_current": 0,
        "remaining": None,
        "status": "pending",
        "message": None,
    }


def _ordered_source_progress_snapshot(source_progress: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    def _sort_key(item: tuple[str, dict[str, Any]]) -> tuple[int, str]:
        key = item[0]
        try:
            return SOURCE_PROGRESS_KEY_ORDER.index(key), key
        except ValueError:
            return len(SOURCE_PROGRESS_KEY_ORDER), key

    return dict(sorted(source_progress.items(), key=_sort_key))


def _resolve_execution_profile(profile: str | None) -> Literal["speed", "balanced", "safe"]:
    normalized = str(profile or "").strip().lower()
    if normalized in {"speed", "balanced", "safe"}:
        return cast(Literal["speed", "balanced", "safe"], normalized)
    return "speed"


def _profile_default_parallelism(
    profile: Literal["speed", "balanced", "safe"],
    stage: Literal["sync", "mirror", "tagging", "crop"],
) -> int:
    defaults: dict[str, dict[str, int]] = {
        "speed": {"sync": 3, "mirror": 12, "tagging": 8, "crop": 8},
        "balanced": {"sync": 2, "mirror": 8, "tagging": 4, "crop": 4},
        "safe": {"sync": 1, "mirror": 4, "tagging": 2, "crop": 2},
    }
    return defaults[profile][stage]


def _profile_default_batch_size(
    profile: Literal["speed", "balanced", "safe"],
    stage: Literal["tagging", "mirror", "crop"],
) -> int:
    defaults: dict[str, dict[str, int]] = {
        "speed": {"tagging": 32, "mirror": 200, "crop": 64},
        "balanced": {"tagging": 24, "mirror": 120, "crop": 48},
        "safe": {"tagging": 16, "mirror": 80, "crop": 24},
    }
    return defaults[profile][stage]


def _resolve_stage_parallelism(
    *,
    request_overrides: dict[str, int] | None,
    stage: Literal["sync", "mirror", "tagging", "crop"],
    default: int,
) -> int:
    if isinstance(request_overrides, dict):
        candidate = request_overrides.get(stage)
        if isinstance(candidate, int) and candidate > 0:
            return candidate
    env_map = {
        "sync": "SYNC_MAX_PARALLEL",
        "mirror": "MIRROR_MAX_PARALLEL",
        "tagging": "TAGGING_MAX_PARALLEL",
        "crop": "CROP_MAX_PARALLEL",
    }
    return _read_positive_int_env(env_map[stage], default)


def _resolve_stage_batch_size(
    *,
    request_overrides: dict[str, int] | None,
    stage: Literal["tagging", "mirror", "crop"],
    default: int,
) -> int:
    if isinstance(request_overrides, dict):
        candidate = request_overrides.get(stage)
        if isinstance(candidate, int) and candidate > 0:
            return candidate
    env_map = {
        "tagging": "TAGGING_BATCH_SIZE",
        "mirror": "MIRROR_BATCH_SIZE",
        "crop": "CROP_BATCH_SIZE",
    }
    return _read_positive_int_env(env_map[stage], default)


def _chunked(items: list[TChunk], size: int) -> list[list[TChunk]]:
    if size <= 0:
        return [items]
    return [items[index : index + size] for index in range(0, len(items), size)]


def _snapshot_payload_sha(payload: dict[str, Any]) -> str:
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _persist_person_getty_snapshot(
    db: SupabaseAdminClient,
    *,
    person_id: str,
    payload: dict[str, Any],
    status: str = "success",
    error: str | None = None,
) -> dict[str, Any]:
    fetched_at = datetime.now(UTC).isoformat()
    payload_sha = _snapshot_payload_sha(payload)
    row = {
        "person_id": person_id,
        "source_id": _GETTY_SOURCE_ID,
        "variant": _GETTY_PERSON_GALLERY_VARIANT,
        "fetched_at": fetched_at,
        "fetch_method": "admin_person_nbcumv_crosswalk",
        "status": status,
        "error": error,
        "payload": payload,
        "payload_sha256": payload_sha,
    }
    latest_resp = (
        db.schema("core").table("person_source_latest").upsert(row, on_conflict="person_id,source_id,variant").execute()
    )
    if getattr(latest_resp, "error", None):
        raise HTTPException(status_code=502, detail=f"Failed to persist Getty person snapshot: {latest_resp.error}")

    history_resp = db.schema("core").table("person_source_history").insert(row).execute()
    if getattr(history_resp, "error", None):
        logger.warning("Failed to persist Getty person snapshot history for %s: %s", person_id, history_resp.error)

    return {
        "person_id": person_id,
        "source_id": _GETTY_SOURCE_ID,
        "variant": _GETTY_PERSON_GALLERY_VARIANT,
        "fetched_at": fetched_at,
        "payload_sha256": payload_sha,
    }


def _is_transient_stage_error(exc: Exception) -> bool:
    lowered = str(exc).lower()
    return any(
        marker in lowered
        for marker in (
            "timeout",
            "timed out",
            "temporarily unavailable",
            "connection reset",
            "broken pipe",
            "network",
            "socket",
        )
    )


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
    filtered_sources: list[SourceType] = [source for source in sources if source not in blocked]
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


def _slugify_gallery_bucket_key(value: str | None) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text


def _extract_getty_caption_show_title(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text or " -- " not in text:
        return None
    candidate = text.split(" -- ", 1)[0].strip()
    return candidate or None


def _find_show_row_by_text_fragment(
    by_alias: dict[str, dict[str, Any]],
    value: str | None,
) -> dict[str, Any] | None:
    direct = _find_show_row_by_alias(by_alias, value)
    if direct:
        return direct
    normalized_text = _normalize_show_lookup_key(value)
    if not normalized_text:
        return None

    best_match: tuple[int, dict[str, Any]] | None = None
    for alias, row in by_alias.items():
        if len(alias) < 4:
            continue
        if alias in normalized_text:
            score = len(alias)
            if best_match is None or score > best_match[0]:
                best_match = (score, row)
    return best_match[1] if best_match else None


def _resolve_gallery_bucket_metadata(
    *,
    asset: dict[str, Any],
    resolved_asset_show: dict[str, Any] | None,
    show_lookup_by_alias: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    event_group_title = str(asset.get("getty_event_group_title") or asset.get("event_name") or "").strip() or None
    title = str(asset.get("search_title") or asset.get("title") or "").strip() or None
    caption = str(asset.get("caption") or asset.get("search_caption") or "").strip() or None

    resolved_show_row = resolved_asset_show if isinstance(resolved_asset_show, dict) else None
    if resolved_show_row is None:
        for candidate in (
            event_group_title,
            title,
            _extract_getty_caption_show_title(caption),
        ):
            resolved_show_row = _find_show_row_by_text_fragment(show_lookup_by_alias, candidate)
            if resolved_show_row:
                break

    event_text = " ".join(value for value in (event_group_title, title) if value).strip()
    normalized_event_text = _normalize_show_lookup_key(event_text)
    if "bravocon" in normalized_event_text:
        return {
            "bucket_type": "bravocon",
            "bucket_key": "bravocon",
            "bucket_label": "BravoCon",
            "resolved_show_id": None,
            "resolved_show_name": None,
            "getty_event_group_title": event_group_title,
            "getty_event_group_id": str(asset.get("event_id") or "").strip() or None,
            "getty_event_group_slug": str(asset.get("event_url_slug") or "").strip() or None,
        }
    if "watch what happens live" in normalized_event_text or re.search(r"\bwwhl\b", normalized_event_text):
        return {
            "bucket_type": "wwhl",
            "bucket_key": "wwhl",
            "bucket_label": "WWHL",
            "resolved_show_id": None,
            "resolved_show_name": "Watch What Happens Live with Andy Cohen",
            "getty_event_group_title": event_group_title,
            "getty_event_group_id": str(asset.get("event_id") or "").strip() or None,
            "getty_event_group_slug": str(asset.get("event_url_slug") or "").strip() or None,
        }
    if isinstance(resolved_show_row, dict):
        resolved_show_id = str(resolved_show_row.get("id") or "").strip() or None
        resolved_show_name = str(resolved_show_row.get("name") or resolved_show_row.get("title") or "").strip() or None
        return {
            "bucket_type": "show",
            "bucket_key": resolved_show_id or _slugify_gallery_bucket_key(resolved_show_name),
            "bucket_label": resolved_show_name,
            "resolved_show_id": resolved_show_id,
            "resolved_show_name": resolved_show_name,
            "getty_event_group_title": event_group_title,
            "getty_event_group_id": str(asset.get("event_id") or "").strip() or None,
            "getty_event_group_slug": str(asset.get("event_url_slug") or "").strip() or None,
        }

    event_label = event_group_title or title
    if event_label:
        return {
            "bucket_type": "event",
            "bucket_key": _slugify_gallery_bucket_key(event_label),
            "bucket_label": event_label,
            "resolved_show_id": None,
            "resolved_show_name": None,
            "getty_event_group_title": event_group_title,
            "getty_event_group_id": str(asset.get("event_id") or "").strip() or None,
            "getty_event_group_slug": str(asset.get("event_url_slug") or "").strip() or None,
        }

    return {
        "bucket_type": "unknown",
        "bucket_key": None,
        "bucket_label": None,
        "resolved_show_id": None,
        "resolved_show_name": None,
        "getty_event_group_title": None,
        "getty_event_group_id": None,
        "getty_event_group_slug": None,
    }


def _import_nbcumv_person_media(
    db: SupabaseAdminClient,
    *,
    person_id: str,
    person_name: str | None,
    show_id: UUID | None,
    show_name: str | None,
    limit: int,
    progress_cb: Callable[[int, int, str], None] | None = None,
) -> dict[str, Any]:
    from api.routers.admin_nbcumv import (
        NbcumvImportItem,
        _ensure_sources,
        _import_single_item,
    )
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration
    from trr_backend.repositories.cast_photos import (
        update_cast_photo_hosted_fields,
        upsert_cast_photos,
    )

    def _summarize_getty_asset(
        asset: dict[str, Any],
        *,
        reason: str,
        image: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        summary = {
            "detail_url": str(asset.get("detail_url") or "").strip() or None,
            "editorial_id": str(asset.get("editorial_id") or "").strip() or None,
            "object_name": str(asset.get("object_name") or "").strip() or None,
            "title": str(asset.get("title") or "").strip() or None,
            "caption": str(asset.get("caption") or "").strip() or None,
            "reason": reason,
        }
        if isinstance(image, dict):
            summary["nbcumv_lbx_id"] = str(image.get("lbx_id") or "").strip() or None
            summary["nbcumv_filename"] = str(image.get("lbx_filename") or "").strip() or None
            summary["nbcumv_show_title"] = str(image.get("lbx_showTitle") or "").strip() or None
        return {key: value for key, value in summary.items() if value not in (None, "", [])}

    result: dict[str, Any] = {
        "fetched": 0,
        "imported": 0,
        "skipped": 0,
        "failed": 0,
        "gallery_links_created": 0,
        "asset_ids": [],
        "errors": [],
        "show_title": str(show_name or "").strip() or None,
        "nbcumv_show_id": None,
        "getty_candidates_total": 0,
        "getty_matched_total": 0,
        "getty_unmatched_total": 0,
        "shared_nbcumv_total": 0,
        "shared_nbcumv_imported": 0,
        "nbcumv_only_total": 0,
        "nbcumv_only_imported": 0,
        "getty_only_imported": 0,
        "getty_snapshot_saved": False,
        "getty_bravo_events": [],
        "getty_broad_events": [],
        "summary_message": None,
    }
    normalized_person_name = str(person_name or "").strip()
    if not normalized_person_name:
        return result
    nbcumv_access_error: str | None = None

    def _normalize_show_title_key(value: str | None) -> str:
        text = str(value or "").strip().lower()
        if not text:
            return ""
        if text.endswith(", the"):
            text = f"the {text[:-5].strip()}"
        text = re.sub(r"[^a-z0-9]+", " ", text)
        return " ".join(text.split())

    def _candidate_show_titles_from_getty(asset: dict[str, Any]) -> list[str]:
        candidates: list[str] = []
        event_group_title = str(asset.get("getty_event_group_title") or asset.get("event_name") or "").strip()
        if event_group_title:
            candidates.append(event_group_title)
        search_title = str(asset.get("search_title") or "").strip()
        if search_title:
            candidates.append(search_title)
        title = str(asset.get("title") or "").strip()
        if title:
            candidates.append(title)
            if " - " in title:
                candidates.append(title.split(" - ", 1)[0].strip())
        caption = str(asset.get("caption") or "").strip()
        if caption and " -- " in caption:
            candidates.append(caption.split(" -- ", 1)[0].strip().title())
        seen: set[str] = set()
        deduped: list[str] = []
        for candidate in candidates:
            normalized = _normalize_show_title_key(candidate)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(candidate)
        return deduped

    def _getty_preview_url(asset: dict[str, Any]) -> str | None:
        for key in (
            "preview_image_url",
            "downloadableCompUrl",
            "galleryHighResCompUrl",
            "highResCompUrl",
            "galleryComp1024Url",
            "compUrl",
            "mainImageUrl",
            "thumbUrl",
            "thumb_url",
        ):
            value = str(asset.get(key) or "").strip()
            if value:
                return value
        return None

    def _getty_dimensions(asset: dict[str, Any]) -> tuple[int | None, int | None]:
        for candidate in (asset.get("assetDimensions"), asset.get("actualMaxDimensions")):
            if isinstance(candidate, dict):
                width = candidate.get("width")
                height = candidate.get("height")
                if isinstance(width, int) and width > 0 and isinstance(height, int) and height > 0:
                    return width, height
            if isinstance(candidate, list):
                for item in candidate:
                    if not isinstance(item, str):
                        continue
                    match = re.search(r"(\d{2,5})\s*x\s*(\d{2,5})", item, flags=re.IGNORECASE)
                    if match:
                        return int(match.group(1)), int(match.group(2))
            if isinstance(candidate, str):
                match = re.search(r"(\d{2,5})\s*x\s*(\d{2,5})", candidate, flags=re.IGNORECASE)
                if match:
                    return int(match.group(1)), int(match.group(2))
        return None, None

    def _getty_people_count(asset: dict[str, Any]) -> int | None:
        raw_value = asset.get("people_count")
        if isinstance(raw_value, int) and raw_value >= 0:
            return raw_value
        if isinstance(raw_value, str) and raw_value.strip().isdigit():
            return int(raw_value.strip())
        return None

    def _filename_nup_set_prefix(filename: str | None) -> str | None:
        cleaned = re.sub(r"\.[A-Z0-9]+$", "", str(filename or "").strip().upper())
        if not cleaned:
            return None
        match = re.match(r"^(NUP_\d+)_", cleaned)
        if not match:
            return None
        return match.group(1)

    def _normalize_nup_match_key(filename: str | None) -> str:
        stem = re.sub(r"\.[A-Z0-9]+$", "", str(filename or "").strip().upper())
        if not stem:
            return ""
        parts = stem.split("_")
        if len(parts) != 3 or parts[0] != "NUP":
            return stem
        frame = parts[2]
        if frame.isdigit():
            frame = str(int(frame))
        return f"{parts[0]}_{parts[1]}_{frame}"

    def _match_nbcumv_image_candidates(
        candidates: list[dict[str, Any]],
        *,
        filename: str,
        allow_nup_set_fallback: bool = False,
    ) -> dict[str, Any] | None:
        normalized_filename = str(filename or "").strip().lower()
        normalized_nup_key = _normalize_nup_match_key(filename)
        if not normalized_filename:
            return None
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            candidate_filename = str(candidate.get("lbx_filename") or "").strip().lower()
            if candidate_filename == normalized_filename:
                return candidate
            if normalized_nup_key and _normalize_nup_match_key(candidate.get("lbx_filename")) == normalized_nup_key:
                return candidate
        if not allow_nup_set_fallback:
            return None
        requested_prefix = _filename_nup_set_prefix(filename)
        if not requested_prefix:
            return None
        matching_prefix_candidates = [
            candidate
            for candidate in candidates
            if isinstance(candidate, dict)
            and _filename_nup_set_prefix(candidate.get("lbx_filename")) == requested_prefix
        ]
        if len(matching_prefix_candidates) == 1:
            return matching_prefix_candidates[0]
        return None

    def _coerce_getty_asset_search_dates(asset: dict[str, Any]) -> list[str]:
        raw_values = (
            asset.get("date_created"),
            asset.get("event_date"),
            asset.get("upload_date"),
            asset.get("date_created_display"),
            asset.get("upload_date_display"),
        )
        parsed_dates: list[str] = []
        seen_dates: set[str] = set()
        for raw_value in raw_values:
            text = str(raw_value or "").strip()
            if not text:
                continue
            candidate_date: str | None = None
            iso_match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", text)
            if iso_match:
                candidate_date = iso_match.group(1)
            else:
                for format_string in ("%B %d, %Y", "%b %d, %Y"):
                    try:
                        candidate_date = datetime.strptime(text, format_string).date().isoformat()
                        break
                    except ValueError:
                        continue
            if candidate_date and candidate_date not in seen_dates:
                seen_dates.add(candidate_date)
                parsed_dates.append(candidate_date)
        return parsed_dates

    def _find_nbcumv_image_from_getty_fallbacks(
        asset: dict[str, Any],
        *,
        filename: str,
        resolved_asset_show: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        show_id_value = str((resolved_asset_show or {}).get("id") or "").strip()
        if not show_id_value:
            return None
        search_dates = _coerce_getty_asset_search_dates(asset)
        allow_nup_set_fallback = bool(normalized_person_name)
        for search_date in search_dates:
            for filter_kwargs, label in (
                (
                    {"created_start": search_date, "created_end": search_date},
                    f"created date {search_date}",
                ),
                (
                    {"live_date_start": search_date, "live_date_end": search_date},
                    f"live date {search_date}",
                ),
            ):
                candidate_images = _safe_nbcumv_call(
                    [],
                    f"NUP fallback search for '{filename}' using {label}",
                    nbcumv_integration.search_images,
                    nbcumv_integration.SearchFilters(
                        show_id=show_id_value,
                        limit=100,
                        **filter_kwargs,
                    ),
                )
                matched_candidate = _match_nbcumv_image_candidates(
                    candidate_images,
                    filename=filename,
                    allow_nup_set_fallback=allow_nup_set_fallback,
                )
                if isinstance(matched_candidate, dict):
                    return matched_candidate
        if normalized_person_name:
            caption_candidates = _safe_nbcumv_call(
                [],
                f"NUP fallback caption search for '{filename}'",
                nbcumv_integration.search_images,
                nbcumv_integration.SearchFilters(
                    show_id=show_id_value,
                    search_caption=normalized_person_name,
                    limit=100,
                ),
            )
            matched_candidate = _match_nbcumv_image_candidates(
                caption_candidates,
                filename=filename,
                allow_nup_set_fallback=allow_nup_set_fallback,
            )
            if isinstance(matched_candidate, dict):
                return matched_candidate
        return None

    def _build_event_asset_candidate(event: dict[str, Any]) -> dict[str, Any] | None:
        asset = event.get("matched_asset") or event.get("representative_asset")
        if not isinstance(asset, dict):
            return None
        merged = dict(asset)
        merged["event_name"] = str(event.get("event_name") or merged.get("event_name") or "").strip() or None
        merged["event_id"] = str(event.get("event_id") or merged.get("event_id") or "").strip() or None
        merged["event_url_slug"] = (
            str(event.get("event_url_slug") or merged.get("event_url_slug") or "").strip() or None
        )
        merged["event_url"] = str(event.get("event_url") or "").strip() or None
        merged["event_date"] = str(event.get("event_date") or merged.get("event_date") or "").strip() or None
        merged["grouped_image_count"] = event.get("grouped_image_count")
        merged["source_query_scope"] = str(event.get("source_query_scope") or "").strip() or None
        merged["event_asset_count_scanned"] = event.get("event_asset_count_scanned")
        merged["asset_samples"] = event.get("asset_samples")
        return merged

    def _build_event_inventory_entry(
        *,
        event: dict[str, Any],
        bucket_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        event_asset = _build_event_asset_candidate(event)
        detail_url = None
        editorial_id = None
        object_name = None
        people_count = None
        if isinstance(event_asset, dict):
            detail_url = str(event_asset.get("detail_url") or "").strip() or None
            editorial_id = str(event_asset.get("editorial_id") or "").strip() or None
            object_name = str(event_asset.get("object_name") or "").strip() or None
            people_count = _getty_people_count(event_asset)
        return {
            "source_query_scope": str(event.get("source_query_scope") or "").strip() or None,
            "event_name": str(event.get("event_name") or "").strip() or None,
            "event_url": str(event.get("event_url") or "").strip() or None,
            "event_id": str(event.get("event_id") or "").strip() or None,
            "event_url_slug": str(event.get("event_url_slug") or "").strip() or None,
            "event_date": str(event.get("event_date") or "").strip() or None,
            "grouped_image_count": event.get("grouped_image_count"),
            "bucket_type": bucket_metadata.get("bucket_type"),
            "bucket_key": bucket_metadata.get("bucket_key"),
            "bucket_label": bucket_metadata.get("bucket_label"),
            "representative_detail_url": detail_url,
            "representative_editorial_id": editorial_id,
            "representative_object_name": object_name,
            "representative_people_count": people_count,
            "person_match_confirmed": bool(event.get("matched_asset")),
            "event_asset_count_scanned": event.get("event_asset_count_scanned"),
            "asset_samples": event.get("asset_samples") if isinstance(event.get("asset_samples"), list) else [],
            "resolution": None,
        }

    def _mark_event_inventory_resolution(
        inventory_by_editorial_id: dict[str, list[dict[str, Any]]],
        asset: dict[str, Any],
        *,
        resolution: str,
    ) -> None:
        editorial_id = str(asset.get("editorial_id") or "").strip()
        if not editorial_id:
            return
        for entry in inventory_by_editorial_id.get(editorial_id, []):
            entry["resolution"] = resolution

    def _build_getty_cast_photo_row(
        asset: dict[str, Any],
        *,
        asset_show_name: str | None,
        crosswalk_reason: str,
    ) -> dict[str, Any] | None:
        editorial_id = str(asset.get("editorial_id") or "").strip()
        preview_url = _getty_preview_url(asset)
        detail_url = str(asset.get("detail_url") or "").strip() or None
        if not editorial_id or not preview_url:
            return None
        width, height = _getty_dimensions(asset)
        people = [
            str(entry.get("text") or "").strip()
            for entry in (asset.get("people") or [])
            if isinstance(entry, dict) and str(entry.get("text") or "").strip()
        ]
        object_name = str(asset.get("object_name") or "").strip() or None
        people_count = _getty_people_count(asset)
        metadata: dict[str, Any] = {
            "getty": asset,
            "getty_only_fallback": True,
            "source_domain": "gettyimages.com",
            "source_url": preview_url,
            "source_page_url": detail_url,
            "original_source_page_url": detail_url,
            "original_source_label": "Getty",
            "crosswalk_reason": crosswalk_reason,
            "source_resolution": "getty_watermark_fallback",
            "getty_details": dict(asset.get("details") or {}) if isinstance(asset.get("details"), dict) else {},
            "getty_tags": (
                list(asset.get("keyword_texts") or []) if isinstance(asset.get("keyword_texts"), list) else []
            ),
            "getty_event_title": str(asset.get("event_name") or "").strip() or None,
            "getty_event_url": str(asset.get("event_url") or "").strip() or None,
            "getty_event_id": str(asset.get("event_id") or "").strip() or None,
            "getty_event_slug": str(asset.get("event_url_slug") or "").strip() or None,
            "getty_event_date": str(asset.get("event_date") or "").strip() or None,
            "grouped_image_count": asset.get("grouped_image_count"),
            "source_query_scope": str(asset.get("source_query_scope") or "").strip() or None,
        }
        if object_name:
            metadata["object_name"] = object_name
        if asset_show_name:
            metadata["show_name"] = asset_show_name
        if people_count is not None:
            metadata["people_count"] = people_count
            metadata["people_count_source"] = "auto"
        if people:
            metadata["people_names"] = people

        # Season from Getty tags
        season_number = None
        for tag in metadata.get("getty_tags") or []:
            tag_match = re.search(r"\bSeason\s+(\d+)\b", str(tag), re.IGNORECASE)
            if tag_match:
                season_number = int(tag_match.group(1))
                break
        if season_number is not None:
            metadata["season_number"] = season_number

        # Episode from caption
        caption_text = str(asset.get("caption") or "").strip()
        ep_match = re.search(r"Episode\s+(\d+)", caption_text, re.IGNORECASE)
        if ep_match:
            metadata["episode_number"] = int(ep_match.group(1))

        # Created date (frontend reads created_at)
        date_created = str(asset.get("date_created") or "").strip()
        if date_created:
            metadata["created_at"] = date_created

        return {
            "person_id": person_id,
            "source": _GETTY_SOURCE_ID,
            "source_image_id": editorial_id,
            "url": preview_url,
            "url_path": urlparse(preview_url).path or None,
            "image_url": preview_url,
            "thumb_url": str(asset.get("thumb_url") or asset.get("thumbUrl") or preview_url).strip() or preview_url,
            "image_url_canonical": preview_url,
            "source_page_url": detail_url,
            "caption": str(asset.get("caption") or "").strip() or None,
            "width": width,
            "height": height,
            "season": season_number,
            "people_names": people or None,
            "title_names": [asset_show_name] if asset_show_name else None,
            "file_name": object_name,
            "metadata": metadata,
        }

    def _fetch_existing_getty_source_ids(editorial_ids: list[str]) -> set[str]:
        existing: set[str] = set()
        if not editorial_ids:
            return existing
        for chunk in _chunked(editorial_ids, 200):
            response = (
                db.schema("core")
                .table("cast_photos")
                .select("source_image_id")
                .eq("person_id", person_id)
                .eq("source", _GETTY_SOURCE_ID)
                .in_("source_image_id", chunk)
                .execute()
            )
            for row in response.data or []:
                if isinstance(row, dict):
                    value = str(row.get("source_image_id") or "").strip()
                    if value:
                        existing.add(value)
        return existing

    def _emit_progress(current: int, total: int, message: str) -> None:
        if progress_cb is None:
            return
        progress_cb(max(0, int(current)), max(0, int(total)), str(message or "").strip())

    def _is_nbcumv_access_error(exc: Exception) -> bool:
        message = str(exc or "").strip().lower()
        if not message:
            return False
        return "nbcumv graphql" in message or "unauthorized" in message or "401" in message

    def _record_nbcumv_access_error(context: str, exc: Exception) -> None:
        nonlocal nbcumv_access_error
        message = str(exc or "").strip() or exc.__class__.__name__
        if nbcumv_access_error == message:
            return
        nbcumv_access_error = message
        logger.warning("NBCUMV unavailable during %s for person_id=%s: %s", context, person_id, message)
        result["errors"].append(f"NBCUMV unavailable during {context}: {message}")
        _emit_progress(0, 0, f"NBCUMV unavailable during {context}. Continuing with Getty-only fallback.")

    def _safe_nbcumv_call(default: Any, context: str, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        if nbcumv_access_error:
            return default
        try:
            return func(*args, **kwargs)
        except RuntimeError as exc:
            if not _is_nbcumv_access_error(exc):
                raise
            _record_nbcumv_access_error(context, exc)
            return default

    resolved_show_name = str(show_name or _get_show_name(db, show_id) or "").strip()
    result["show_title"] = resolved_show_name or None

    def _resolve_requested_nbcumv_show() -> dict[str, Any] | None:
        if not resolved_show_name:
            return None
        return _safe_nbcumv_call(
            None,
            f"show resolution for '{resolved_show_name}'",
            nbcumv_integration.resolve_show_by_title,
            resolved_show_name,
        )

    bravo_search_phrase = f"{normalized_person_name} Bravo".strip()
    search_phrase = bravo_search_phrase
    _emit_progress(0, 0, f"Searching Getty for '{bravo_search_phrase}'...")
    getty_assets = getty_integration.search_editorial_assets(
        bravo_search_phrase,
        limit=max(1, int(limit)),
        progress_cb=_emit_progress,
    )
    if not getty_assets:
        fallback_phrase = normalized_person_name
        _emit_progress(0, 0, f"No Getty results for '{bravo_search_phrase}'. Retrying with '{fallback_phrase}'...")
        getty_assets = getty_integration.search_editorial_assets(
            fallback_phrase,
            limit=max(1, int(limit)),
            progress_cb=_emit_progress,
            query_params={"sort": "best"},
        )
        search_phrase = fallback_phrase
    result["getty_candidates_total"] = len(getty_assets)

    bravo_grouped_phrase = f"{normalized_person_name} Bravo".strip()
    _emit_progress(0, 0, f"Collecting Getty grouped events for '{bravo_grouped_phrase}'...")
    bravo_grouped_events = getty_integration.search_grouped_events(
        bravo_grouped_phrase,
        limit=max(1, int(limit)),
        person_name=normalized_person_name,
        source_query_scope="bravo",
    )
    _emit_progress(0, 0, f"Collecting Getty grouped events for '{normalized_person_name}'...")
    broad_grouped_events = getty_integration.search_grouped_events(
        normalized_person_name,
        limit=max(1, int(limit)),
        person_name=normalized_person_name,
        person_match_required=True,
        minimum_grouped_image_count=2,
        query_params={"sort": "best"},
        source_query_scope="broad",
    )

    _ensure_sources(db)
    matched_assets: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any], str]] = []
    matched_summaries: list[dict[str, Any]] = []
    unmatched_assets: list[dict[str, Any]] = []
    filtered_out_assets: list[dict[str, Any]] = []
    getty_only_rows: list[dict[str, Any]] = []
    seen_lbx_ids: set[str] = set()
    normalized_show = _normalize_show_title_key(resolved_show_name)
    resolved_getty_show_cache: dict[str, dict[str, Any] | None] = {}
    show_image_indexes: dict[str, dict[str, dict[str, Any]]] = {}
    _, show_lookup_by_alias, _ = _build_show_lookup_maps(db)
    requested_nbcumv_show = _resolve_requested_nbcumv_show()
    event_inventory_by_editorial_id: dict[str, list[dict[str, Any]]] = {}

    def _resolve_direct_nbcumv_shows() -> list[dict[str, Any]]:
        if isinstance(requested_nbcumv_show, dict):
            requested_show_id = str(requested_nbcumv_show.get("id") or "").strip()
            requested_show_title = str(requested_nbcumv_show.get("title") or "").strip()
            if requested_show_id:
                result["nbcumv_show_id"] = requested_show_id
            if requested_show_title and not result.get("show_title"):
                result["show_title"] = requested_show_title
            return [requested_nbcumv_show]
        if resolved_show_name:
            return []

        resolved_shows: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        candidate_show_names = list(_load_person_credit_show_names(db, person_id))
        for discovered_show_title in _safe_nbcumv_call(
            [],
            f"CloudSearch show discovery for '{normalized_person_name}'",
            nbcumv_integration.discover_person_show_titles,
            normalized_person_name,
            limit=max(25, int(limit)),
        ):
            cleaned_show_title = str(discovered_show_title or "").strip()
            if cleaned_show_title:
                candidate_show_names.append(cleaned_show_title)

        for credited_show_name in candidate_show_names:
            resolved_show = _safe_nbcumv_call(
                None,
                f"credited show resolution for '{credited_show_name}'",
                nbcumv_integration.resolve_show_by_title,
                credited_show_name,
            )
            if not isinstance(resolved_show, dict):
                continue
            resolved_show_id = str(resolved_show.get("id") or "").strip()
            resolved_show_title = (
                str(resolved_show.get("title") or resolved_show.get("name") or "").strip() or credited_show_name
            )
            dedupe_key = resolved_show_id or _normalize_show_title_key(resolved_show_title)
            if not dedupe_key or dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            normalized_show_row = dict(resolved_show)
            if resolved_show_title:
                normalized_show_row.setdefault("title", resolved_show_title)
            resolved_shows.append(normalized_show_row)
        if len(resolved_shows) == 1:
            only_show_id = str(resolved_shows[0].get("id") or "").strip()
            only_show_title = str(resolved_shows[0].get("title") or "").strip()
            if only_show_id:
                result["nbcumv_show_id"] = only_show_id
            if only_show_title and not result.get("show_title"):
                result["show_title"] = only_show_title
        return resolved_shows

    def _resolve_asset_show(asset: dict[str, Any]) -> dict[str, Any] | None:
        for candidate in _candidate_show_titles_from_getty(asset):
            if candidate in resolved_getty_show_cache:
                show = resolved_getty_show_cache[candidate]
            else:
                show = _safe_nbcumv_call(
                    None,
                    f"Getty show resolution for '{candidate}'",
                    nbcumv_integration.resolve_show_by_title,
                    candidate,
                )
                resolved_getty_show_cache[candidate] = show
            if show:
                return show
        return None

    def _lookup_show_index(show_id_value: str) -> dict[str, dict[str, Any]]:
        normalized_id = str(show_id_value or "").strip()
        if normalized_id not in show_image_indexes:
            show_image_indexes[normalized_id] = _safe_nbcumv_call(
                {},
                f"show image index for '{normalized_id}'",
                nbcumv_integration.build_show_image_index,
                normalized_id,
            )
        return show_image_indexes[normalized_id]

    def _build_direct_nbcumv_bucket_metadata(
        image: dict[str, Any],
        *,
        requested_show: dict[str, Any] | None,
    ) -> dict[str, Any]:
        image_show_title = str(image.get("lbx_showTitle") or "").strip() or None
        image_headline = str(image.get("lbx_headline") or image.get("headline") or "").strip() or None
        requested_show_id = str((requested_show or {}).get("id") or "").strip()
        resolved_show_row = None
        for candidate in (
            image_show_title,
            image_headline,
            str((requested_show or {}).get("title") or "").strip() or None,
            resolved_show_name or None,
        ):
            resolved_show_row = _find_show_row_by_text_fragment(show_lookup_by_alias, candidate)
            if resolved_show_row:
                break
        if resolved_show_row is None and requested_show_id and isinstance(requested_show, dict):
            resolved_show_row = requested_show
        synthetic_asset = {
            "title": image_headline
            or image_show_title
            or str((requested_show or {}).get("title") or "").strip()
            or resolved_show_name,
            "caption": str(image.get("lbx_caption") or "").strip() or None,
            "event_name": image_headline,
        }
        bucket_metadata = _resolve_gallery_bucket_metadata(
            asset=synthetic_asset,
            resolved_asset_show=resolved_show_row,
            show_lookup_by_alias=show_lookup_by_alias,
        )
        grouped_image_count = image.get("grouped_image_count")
        if isinstance(grouped_image_count, int) and grouped_image_count > 0:
            bucket_metadata["grouped_image_count"] = grouped_image_count
        elif isinstance(grouped_image_count, str) and grouped_image_count.strip().isdigit():
            bucket_metadata["grouped_image_count"] = int(grouped_image_count.strip())
        return bucket_metadata

    def _run_direct_nbcumv_caption_search(
        requested_shows: list[dict[str, Any]],
        *,
        getty_candidates_present: bool,
    ) -> list[tuple[dict[str, Any], dict[str, Any], dict[str, Any], str]]:
        if nbcumv_access_error:
            if not getty_candidates_present:
                result["summary_message"] = (
                    f"No Getty candidates found and NBCUMV is unavailable for '{normalized_person_name}'."
                )
            return []

        valid_requested_shows = [
            show for show in requested_shows if isinstance(show, dict) and str(show.get("id") or "").strip()
        ]
        explicit_show_context = bool(str(show_id or "").strip() or resolved_show_name)
        search_targets: list[dict[str, Any]] = []
        if valid_requested_shows:
            search_targets.extend(valid_requested_shows)
        if not explicit_show_context:
            search_targets.append({"id": None, "title": "All NBCUMV"})

        if not search_targets:
            if resolved_show_name and not getty_candidates_present:
                result["summary_message"] = (
                    f"No Getty candidates found and NBCUMV direct search could not resolve show '{resolved_show_name}'."
                )
            elif not getty_candidates_present:
                result["summary_message"] = (
                    "No Getty candidates found and NBCUMV direct search "
                    f"requires show context or credited shows for '{normalized_person_name}'."
                )
            return []

        if len(search_targets) == 1 and str(search_targets[0].get("id") or "").strip():
            only_show_title = str(search_targets[0].get("title") or "").strip() or resolved_show_name
            _emit_progress(
                0,
                0,
                (
                    (
                        f"No Getty candidates found. Searching NBCUMV directly for '{normalized_person_name}' "
                        f"in '{only_show_title}'..."
                    )
                    if not getty_candidates_present
                    else (
                        f"Supplementing Getty matches with NBCUMV caption search for '{normalized_person_name}' "
                        f"in '{only_show_title}'..."
                    )
                ),
            )
        elif len(search_targets) == 1:
            _emit_progress(
                0,
                0,
                (
                    f"No Getty candidates found. Searching all NBCUMV directly for '{normalized_person_name}'..."
                    if not getty_candidates_present
                    else (
                        "Supplementing Getty matches with an all-NBCUMV caption search "
                        f"for '{normalized_person_name}'..."
                    )
                ),
            )
        else:
            _emit_progress(
                0,
                0,
                (
                    (
                        f"No Getty candidates found. Searching NBCUMV directly for '{normalized_person_name}' "
                        f"across {len(search_targets)} search scopes..."
                    )
                    if not getty_candidates_present
                    else (
                        f"Supplementing Getty matches with NBCUMV caption search for '{normalized_person_name}' "
                        f"across {len(search_targets)} search scopes..."
                    )
                ),
            )

        matched_direct_images: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any], str]] = []
        seen_direct_keys: set[str] = set()
        attempted_show_titles: list[str] = []

        for show_index, requested_show in enumerate(search_targets, start=1):
            requested_show_id = str(requested_show.get("id") or "").strip()
            requested_show_title = str(requested_show.get("title") or "").strip() or resolved_show_name
            if requested_show_title:
                attempted_show_titles.append(requested_show_title)
            _emit_progress(
                show_index - 1,
                len(search_targets),
                (
                    f"Searching NBCUMV caption matches in "
                    f"'{requested_show_title or requested_show_id or 'All NBCUMV'}' "
                    f"({show_index}/{len(search_targets)})..."
                ),
            )
            if requested_show_id:
                direct_images = _safe_nbcumv_call(
                    [],
                    f"direct show catalog search in '{requested_show_title or requested_show_id}'",
                    nbcumv_integration.search_person_show_catalog,
                    normalized_person_name,
                    show_id=requested_show_id,
                    limit=max(1, int(limit)),
                )
            else:
                direct_images = _safe_nbcumv_call(
                    [],
                    f"direct person search in '{requested_show_title or requested_show_id}'",
                    nbcumv_integration.search_person_images,
                    normalized_person_name,
                    show_id=None,
                    limit=max(1, int(limit)),
                )
            for image in direct_images:
                dedupe_key = str(image.get("lbx_id") or "").strip() or str(image.get("lbx_filename") or "").strip()
                if dedupe_key and dedupe_key in seen_direct_keys:
                    continue
                if dedupe_key:
                    seen_direct_keys.add(dedupe_key)
                lbx_id = str(image.get("lbx_id") or "").strip()
                if lbx_id and lbx_id in seen_lbx_ids:
                    continue
                filename = str(image.get("lbx_filename") or "").strip()
                if not filename:
                    result["failed"] += 1
                    result["errors"].append("NBCUMV direct search returned an image without a filename.")
                    continue
                if lbx_id:
                    seen_lbx_ids.add(lbx_id)
                bucket_metadata = _build_direct_nbcumv_bucket_metadata(image, requested_show=requested_show)
                bucket_metadata = dict(bucket_metadata)
                bucket_metadata["source_resolution"] = "nbcumv_only"
                matched_direct_images.append(({}, image, bucket_metadata, "nbcumv_only"))
                _emit_progress(
                    len(matched_direct_images),
                    max(len(matched_direct_images), len(search_targets)),
                    f"Queued NBCUMV direct match {len(matched_direct_images)}: {filename}",
                )

        if not matched_direct_images:
            if not getty_candidates_present:
                attempted_label = ", ".join(attempted_show_titles[:3])
                if len(attempted_show_titles) > 3:
                    attempted_label = f"{attempted_label}, +{len(attempted_show_titles) - 3} more"
                result["summary_message"] = (
                    "No Getty candidates found and NBCUMV direct search returned no caption matches"
                    + (f" across {attempted_label}." if attempted_label else ".")
                )
                _emit_progress(0, 0, result["summary_message"])
            return []
        return matched_direct_images

    scoped_assets: list[tuple[dict[str, Any], dict[str, Any] | None, str | None, dict[str, Any]]] = []
    if normalized_show:
        _emit_progress(0, len(getty_assets), f"Filtering {len(getty_assets)} Getty assets to '{resolved_show_name}'...")
    for asset in getty_assets:
        resolved_asset_show = _resolve_asset_show(asset)
        resolved_asset_show_title = (
            str(resolved_asset_show.get("title") or "").strip() if isinstance(resolved_asset_show, dict) else ""
        )
        if normalized_show:
            show_candidates = _candidate_show_titles_from_getty(asset)
            candidate_match = any(
                (
                    normalized_show in _normalize_show_title_key(candidate)
                    or _normalize_show_title_key(candidate) in normalized_show
                )
                for candidate in show_candidates
            )
            resolved_show_match = bool(resolved_asset_show_title) and (
                normalized_show in _normalize_show_title_key(resolved_asset_show_title)
                or _normalize_show_title_key(resolved_asset_show_title) in normalized_show
            )
            if not candidate_match and not resolved_show_match:
                filtered_out_assets.append(_summarize_getty_asset(asset, reason="requested_show_mismatch"))
                continue
        bucket_metadata = _resolve_gallery_bucket_metadata(
            asset=asset,
            resolved_asset_show=resolved_asset_show,
            show_lookup_by_alias=show_lookup_by_alias,
        )
        scoped_assets.append((asset, resolved_asset_show, resolved_asset_show_title or None, bucket_metadata))

    broad_event_assets: list[tuple[dict[str, Any], dict[str, Any] | None, str | None, dict[str, Any]]] = []
    seen_editorial_ids = {
        str(asset.get("editorial_id") or "").strip()
        for asset, _, _, _ in scoped_assets
        if str(asset.get("editorial_id") or "").strip()
    }

    def _capture_grouped_event_inventory(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        captured: list[dict[str, Any]] = []
        for event in events:
            event_asset = _build_event_asset_candidate(event)
            synthetic_asset = event_asset or {
                "title": event.get("event_name"),
                "caption": None,
                "event_name": event.get("event_name"),
            }
            resolved_event_show = _resolve_asset_show(synthetic_asset) if isinstance(synthetic_asset, dict) else None
            resolved_event_show_title = (
                str(resolved_event_show.get("title") or "").strip() if isinstance(resolved_event_show, dict) else ""
            )
            bucket_metadata = _resolve_gallery_bucket_metadata(
                asset=synthetic_asset if isinstance(synthetic_asset, dict) else {},
                resolved_asset_show=resolved_event_show,
                show_lookup_by_alias=show_lookup_by_alias,
            )
            inventory_entry = _build_event_inventory_entry(event=event, bucket_metadata=bucket_metadata)
            editorial_id = str(inventory_entry.get("representative_editorial_id") or "").strip()
            if editorial_id:
                event_inventory_by_editorial_id.setdefault(editorial_id, []).append(inventory_entry)
            captured.append(inventory_entry)
            if (
                bucket_metadata.get("bucket_type") == "event"
                and isinstance(event_asset, dict)
                and editorial_id
                and editorial_id not in seen_editorial_ids
            ):
                broad_event_assets.append(
                    (
                        event_asset,
                        resolved_event_show,
                        resolved_event_show_title or None,
                        bucket_metadata,
                    )
                )
                seen_editorial_ids.add(editorial_id)
        return captured

    result["getty_bravo_events"] = _capture_grouped_event_inventory(bravo_grouped_events)
    result["getty_broad_events"] = _capture_grouped_event_inventory(broad_grouped_events)

    combined_assets = scoped_assets + broad_event_assets
    result["getty_candidates_total"] = len(combined_assets)
    if not combined_assets:
        direct_nbcumv_shows = _resolve_direct_nbcumv_shows()
        direct_matches = _run_direct_nbcumv_caption_search(
            direct_nbcumv_shows,
            getty_candidates_present=False,
        )
        matched_assets.extend(direct_matches)
        if direct_matches:
            result["summary_message"] = (
                f"NBCUMV direct caption search queued {len(matched_assets)} match"
                f"{'es' if len(matched_assets) != 1 else ''}."
            )
    match_total = len(combined_assets)
    _emit_progress(0, match_total, f"Matching {match_total} Getty assets against NBCUMV...")
    for match_index, (asset, resolved_asset_show, resolved_asset_show_title, bucket_metadata) in enumerate(
        combined_assets,
        start=1,
    ):
        filename = str(asset.get("object_name") or "").strip()
        _emit_progress(
            match_index - 1,
            match_total,
            f"Matching Getty asset {match_index}/{match_total}: {filename or asset.get('editorial_id') or 'unknown'}",
        )
        if not filename:
            unmatched_assets.append(_summarize_getty_asset(asset, reason="missing_object_name"))
            _mark_event_inventory_resolution(event_inventory_by_editorial_id, asset, resolution="missing_object_name")
            continue
        image = None
        if isinstance(resolved_asset_show, dict):
            show_id_value = str(resolved_asset_show.get("id") or "").strip()
            if show_id_value:
                image = _lookup_show_index(show_id_value).get(filename.lower())
        if not isinstance(image, dict):
            image = _safe_nbcumv_call(
                None,
                f"identity lookup for '{filename}'",
                nbcumv_integration.fetch_image_by_identity,
                filename=filename,
            )
        if not isinstance(image, dict):
            image = _find_nbcumv_image_from_getty_fallbacks(
                asset,
                filename=filename,
                resolved_asset_show=resolved_asset_show,
            )
        if not isinstance(image, dict):
            unmatched_assets.append(_summarize_getty_asset(asset, reason="no_nbcumv_match"))
            getty_row = _build_getty_cast_photo_row(
                asset,
                asset_show_name=(
                    str(bucket_metadata.get("resolved_show_name") or "").strip()
                    or resolved_asset_show_title
                    or resolved_show_name
                    or None
                ),
                crosswalk_reason="nbcumv_unavailable" if nbcumv_access_error else "no_nbcumv_match",
            )
            if getty_row is not None:
                metadata = getty_row.get("metadata")
                if isinstance(metadata, dict):
                    metadata["gallery_bucket"] = dict(bucket_metadata)
                    metadata.update(dict(bucket_metadata))
                    metadata["source_resolution"] = "getty_watermark_fallback"
                if bucket_metadata.get("bucket_type") == "show" and bucket_metadata.get("resolved_show_name"):
                    getty_row["title_names"] = [str(bucket_metadata["resolved_show_name"])]
                getty_only_rows.append(getty_row)
                _mark_event_inventory_resolution(
                    event_inventory_by_editorial_id,
                    asset,
                    resolution="getty_watermark_fallback",
                )
            _emit_progress(
                match_index,
                match_total,
                f"No NBCUMV match for Getty asset {match_index}/{match_total}; keeping Getty preview.",
            )
            continue
        image_show_title = str(image.get("lbx_showTitle") or "").strip()
        if normalized_show:
            image_show_folded = _normalize_show_title_key(image_show_title)
            if normalized_show not in image_show_folded and image_show_folded not in normalized_show:
                filtered_out_assets.append(_summarize_getty_asset(asset, reason="show_mismatch", image=image))
                _emit_progress(
                    match_index,
                    match_total,
                    (
                        f"Skipped {filename}: NBCUMV show '{image_show_title or 'unknown'}' "
                        f"did not match '{resolved_show_name}'."
                    ),
                )
                continue
        lbx_id = str(image.get("lbx_id") or "").strip()
        if not lbx_id or lbx_id in seen_lbx_ids:
            unmatched_assets.append(
                _summarize_getty_asset(
                    asset,
                    reason="duplicate_or_invalid_nbcumv_asset",
                    image=image,
                )
            )
            _emit_progress(match_index, match_total, f"Skipped duplicate or invalid NBCUMV asset for {filename}.")
            continue
        seen_lbx_ids.add(lbx_id)
        _mark_event_inventory_resolution(event_inventory_by_editorial_id, asset, resolution="nbcumv_matched")
        matched_bucket_metadata = dict(bucket_metadata)
        matched_bucket_metadata["source_resolution"] = "nbcumv_preferred_shared"
        matched_assets.append((asset, image, matched_bucket_metadata, "nbcumv_preferred_shared"))
        matched_summaries.append(_summarize_getty_asset(asset, reason="matched", image=image))
        _emit_progress(match_index, match_total, f"Matched NBCUMV asset {len(matched_assets)}: {filename}")

    if combined_assets:
        direct_nbcumv_matches = _run_direct_nbcumv_caption_search(
            _resolve_direct_nbcumv_shows(),
            getty_candidates_present=True,
        )
        if direct_nbcumv_matches:
            matched_assets.extend(direct_nbcumv_matches)

    shared_nbcumv_total = sum(1 for *_rest, resolution in matched_assets if resolution == "nbcumv_preferred_shared")
    nbcumv_only_total = sum(1 for *_rest, resolution in matched_assets if resolution == "nbcumv_only")
    result["fetched"] = len(matched_assets)
    result["getty_matched_total"] = shared_nbcumv_total
    result["getty_unmatched_total"] = len(unmatched_assets)
    result["shared_nbcumv_total"] = shared_nbcumv_total
    result["nbcumv_only_total"] = nbcumv_only_total
    snapshot_payload = {
        "person_id": person_id,
        "person_name": normalized_person_name,
        "search_phrase": search_phrase,
        "requested_limit": max(1, int(limit)),
        "show_filter": {
            "show_id": str(show_id) if show_id else None,
            "show_name": resolved_show_name or None,
        },
        "candidate_count": len(combined_assets),
        "raw_candidate_count": len(getty_assets),
        "matched_count": shared_nbcumv_total,
        "shared_nbcumv_total": shared_nbcumv_total,
        "nbcumv_only_total": nbcumv_only_total,
        "unmatched_count": len(unmatched_assets),
        "filtered_out_count": len(filtered_out_assets),
        "bravo_events": result["getty_bravo_events"],
        "broad_events": result["getty_broad_events"],
        "matched": matched_summaries,
        "unmatched": unmatched_assets,
        "filtered_out": filtered_out_assets,
    }
    if getty_assets or bravo_grouped_events or broad_grouped_events:
        try:
            _persist_person_getty_snapshot(
                db,
                person_id=person_id,
                payload=snapshot_payload,
                status="success",
            )
            result["getty_snapshot_saved"] = True
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to persist Getty person snapshot for %s: %s", person_id, exc)
            result["errors"].append(f"Getty snapshot persistence failed: {exc}")

    if getty_only_rows:
        editorial_ids = [
            str(row.get("source_image_id") or "").strip()
            for row in getty_only_rows
            if str(row.get("source_image_id") or "").strip()
        ]
        existing_getty_ids = _fetch_existing_getty_source_ids(editorial_ids)
        _emit_progress(0, len(getty_only_rows), f"Importing {len(getty_only_rows)} Getty-only fallback photos...")
        upserted_rows = upsert_cast_photos(db, getty_only_rows, dedupe_on="source_image_id")
        source_rows_by_image_id = {
            str(row.get("source_image_id") or "").strip(): row
            for row in getty_only_rows
            if str(row.get("source_image_id") or "").strip()
        }
        for upserted_row in upserted_rows:
            row_id = str(upserted_row.get("id") or "").strip()
            source_image_id = str(upserted_row.get("source_image_id") or "").strip()
            source_row = source_rows_by_image_id.get(source_image_id)
            preview_url = str((source_row or {}).get("url") or "").strip()
            if not row_id or not preview_url:
                continue
            try:
                update_cast_photo_hosted_fields(
                    db,
                    row_id,
                    {"hosted_url": preview_url, "hosted_content_type": "image/jpeg"},
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to set Getty fallback hosted fields for %s: %s", row_id, exc)
        result["getty_only_imported"] = sum(
            1 for row in upserted_rows if str(row.get("source_image_id") or "").strip() not in existing_getty_ids
        )
        result["skipped"] += max(0, len(upserted_rows) - int(result["getty_only_imported"]))
        imported_getty_count = int(result["getty_only_imported"])
        existing_getty_count = len(upserted_rows) - imported_getty_count
        _emit_progress(
            len(getty_only_rows),
            len(getty_only_rows),
            (f"Imported Getty-only fallback photos ({imported_getty_count} new, {existing_getty_count} existing)."),
        )

    if not matched_assets and not getty_only_rows:
        if result["summary_message"] is None and nbcumv_access_error:
            result["summary_message"] = (
                f"NBCUMV unavailable for '{normalized_person_name}' and no Getty fallback rows were importable."
            )
        return result

    total = len(matched_assets)
    if total > 0:
        _emit_progress(0, total, f"Importing {total} NBCUMV assets...")
    for index, (asset, image, bucket_metadata, source_resolution) in enumerate(matched_assets, start=1):
        filename = str(image.get("lbx_filename") or "").strip()
        lbx_id = str(image.get("lbx_id") or "").strip()
        _emit_progress(index - 1, total, f"Importing NBCUMV {index}/{total}: {filename or lbx_id}")
        if not filename or not lbx_id:
            result["failed"] += 1
            result["errors"].append("NBCUMV item missing lbx_id or filename.")
            continue
        item_bucket_metadata = dict(bucket_metadata)
        item_bucket_metadata["source_resolution"] = source_resolution
        try:
            import_result = _import_single_item(
                db=db,
                item=NbcumvImportItem(
                    lbx_id=lbx_id,
                    lbx_filename=filename,
                    location=image.get("location"),
                    nbcumv_image=image,
                    show_ids=[value for value in image.get("showIds") or [] if isinstance(value, str)],
                    link_show_ids=[show_id] if show_id else [],
                    getty_detail_url=str(asset.get("detail_url") or "").strip() or None,
                    gallery_bucket=item_bucket_metadata,
                    person_ids=[UUID(person_id)],
                ),
                assign_people=True,
                people_index={},
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("NBCUMV person import failed person_id=%s lbx_id=%s", person_id, lbx_id)
            result["failed"] += 1
            result["errors"].append(f"{filename or lbx_id}: {exc}")
            continue

        if import_result.get("asset_id"):
            result["asset_ids"].append(str(import_result["asset_id"]))
        created_links = len(import_result.get("created_person_ids") or []) + len(
            import_result.get("created_show_ids") or []
        )
        result["gallery_links_created"] += created_links
        if import_result.get("already_imported") and created_links == 0:
            result["skipped"] += 1
        else:
            result["imported"] += 1
            if source_resolution == "nbcumv_preferred_shared":
                result["shared_nbcumv_imported"] += 1
            elif source_resolution == "nbcumv_only":
                result["nbcumv_only_imported"] += 1
        _emit_progress(index, total, f"Imported NBCUMV {index}/{total}: {filename or lbx_id}")
    result["fetched"] = len(matched_assets) + len(getty_only_rows)
    if result["summary_message"] is None:
        total_candidate_count = max(
            int(result.get("getty_candidates_total") or 0),
            len(matched_assets) + int(result.get("skipped") or 0) + int(result.get("failed") or 0),
        )
        summary_prefix = (
            "Getty complete with NBCUMV unavailable: " if nbcumv_access_error else "Getty/NBCUMV complete: "
        )
        result["summary_message"] = (
            summary_prefix
            + f"{int(result.get('shared_nbcumv_imported') or 0)} shared via NBCUMV, "
            f"{int(result.get('nbcumv_only_imported') or 0)} NBCUMV-only, "
            f"{int(result.get('getty_only_imported') or 0)} Getty-only, "
            f"{int(result.get('skipped') or 0)} skipped, {int(result.get('failed') or 0)} failed"
            + (f" across {total_candidate_count} candidates." if total_candidate_count > 0 else ".")
        )
    return result


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
        show_resp = db.schema("core").table("shows").select("imdb_id").eq("id", show_id_str).limit(1).execute()
        show_rows = show_resp.data or []
        if show_rows and isinstance(show_rows[0], dict):
            show_imdb_id = show_rows[0].get("imdb_id")
            if isinstance(show_imdb_id, str) and show_imdb_id.strip():
                imdb_ids.add(show_imdb_id.strip().lower())
    except Exception:  # noqa: BLE001
        pass

    try:
        episode_resp = (
            db.schema("core").table("episodes").select("external_ids").eq("show_id", show_id_str).limit(5000).execute()
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


def _empty_auto_count_diagnostics() -> dict[str, int]:
    return dict.fromkeys(AUTO_COUNT_DIAGNOSTIC_FIELDS, 0)


def _empty_stage_row_stats() -> dict[str, int]:
    return {
        "attempted_rows": 0,
        "skipped_existing_rows": 0,
        "retry_attempted_rows": 0,
        "retry_succeeded_rows": 0,
    }


def _merge_stage_row_stats(target: dict[str, int], source: dict[str, int]) -> None:
    for key in ("attempted_rows", "skipped_existing_rows", "retry_attempted_rows", "retry_succeeded_rows"):
        target[key] = int(target.get(key, 0)) + int(source.get(key, 0))


def _record_stage_row_stats(
    stats: dict[str, int] | None,
    *,
    attempted_rows: int = 0,
    skipped_existing_rows: int = 0,
    retry_attempted_rows: int = 0,
    retry_succeeded_rows: int = 0,
) -> None:
    if not isinstance(stats, dict):
        return
    stats["attempted_rows"] = int(stats.get("attempted_rows", 0)) + int(max(attempted_rows, 0))
    stats["skipped_existing_rows"] = int(stats.get("skipped_existing_rows", 0)) + int(max(skipped_existing_rows, 0))
    stats["retry_attempted_rows"] = int(stats.get("retry_attempted_rows", 0)) + int(max(retry_attempted_rows, 0))
    stats["retry_succeeded_rows"] = int(stats.get("retry_succeeded_rows", 0)) + int(max(retry_succeeded_rows, 0))


def _merge_counter_fields(target: dict[str, int], source: dict[str, int], fields: tuple[str, ...]) -> None:
    for key in fields:
        target[key] = int(target.get(key, 0)) + int(source.get(key, 0))


def _owner_face_match_similarity_min() -> float:
    raw = str(os.getenv("OWNER_FACE_MATCH_SIMILARITY_MIN") or "").strip()
    if not raw:
        return OWNER_FACE_MATCH_SIMILARITY_MIN_DEFAULT
    try:
        value = float(raw)
    except ValueError:
        return OWNER_FACE_MATCH_SIMILARITY_MIN_DEFAULT
    return max(0.0, min(1.0, value))


def _build_auto_count_row_error_counts(diagnostics: dict[str, int]) -> dict[str, int]:
    return {
        "detect_failed": int(diagnostics.get("auto_detect_failed_rows", 0)),
        "persist_failed": int(diagnostics.get("auto_persist_failed_rows", 0)),
        "crop_cache_failed": int(diagnostics.get("auto_crop_cache_failed_rows", 0)),
    }


def _build_failed_parts_summary(
    *,
    metadata_enrichment_failed: int = 0,
    auto_counts_failed: int = 0,
    row_error_counts: dict[str, int] | None = None,
    text_overlay_failed: int = 0,
    text_overlay_failure_reasons: dict[str, int] | None = None,
    centering_failed: int = 0,
    resize_failed: int = 0,
    resize_crop_failed: int = 0,
) -> list[dict[str, Any]]:
    failed_parts: list[dict[str, Any]] = []
    if int(metadata_enrichment_failed) > 0:
        failed_parts.append(
            {
                "part": "metadata_enrichment",
                "failed": int(metadata_enrichment_failed),
            }
        )
    if int(auto_counts_failed) > 0:
        part: dict[str, Any] = {
            "part": "people_count_face_crops",
            "failed": int(auto_counts_failed),
        }
        if isinstance(row_error_counts, dict):
            normalized_row_errors = {
                key: int(value)
                for key, value in row_error_counts.items()
                if isinstance(value, (int, float)) and int(value) > 0
            }
            if normalized_row_errors:
                part["row_error_counts"] = normalized_row_errors
        failed_parts.append(part)
    if int(text_overlay_failed) > 0:
        part = {
            "part": "text_overlay",
            "failed": int(text_overlay_failed),
        }
        if isinstance(text_overlay_failure_reasons, dict):
            reason_counts = {
                key: int(value)
                for key, value in text_overlay_failure_reasons.items()
                if isinstance(value, (int, float)) and int(value) > 0
            }
            if reason_counts:
                part["reason_counts"] = reason_counts
        failed_parts.append(part)
    if int(centering_failed) > 0:
        failed_parts.append(
            {
                "part": "centering_cropping",
                "failed": int(centering_failed),
            }
        )
    if int(resize_failed) > 0 or int(resize_crop_failed) > 0:
        failed_parts.append(
            {
                "part": "resizing",
                "failed": int(resize_failed) + int(resize_crop_failed),
                "base_failed": int(resize_failed),
                "crop_failed": int(resize_crop_failed),
            }
        )
    return failed_parts


def _has_face_metadata_backfill_needed(metadata: Any) -> bool:
    if not isinstance(metadata, dict):
        return True
    face_boxes = metadata.get("face_boxes")
    face_crops = metadata.get("face_crops")
    return not isinstance(face_boxes, list) or not isinstance(face_crops, list)


def _normalize_uuid_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate:
        return None
    try:
        return str(UUID(candidate))
    except (ValueError, TypeError, AttributeError):
        return None


def _is_trr_show_eligible(
    db: SupabaseAdminClient,
    *,
    metadata: Any,
    request_show_id: UUID | None = None,
    request_show_name: str | None = None,
    show_lookup_by_alias: dict[str, dict[str, Any]] | None = None,
    show_exists_cache: dict[str, bool] | None = None,
    show_name_cache: dict[str, str | None] | None = None,
) -> bool:
    # Use shared repository logic first.
    if is_trr_show_eligible_shared(
        db,
        metadata=metadata,
        request_show_id=request_show_id,
        request_show_name=request_show_name,
        show_exists_cache=show_exists_cache,
        show_name_cache=show_name_cache,
    ):
        return True

    # Backward-compatible alias fallback for contexts that only provide a show name.
    metadata_obj = metadata if isinstance(metadata, dict) else {}
    requested_show_id = _normalize_uuid_text(str(request_show_id) if request_show_id is not None else None)
    resolved_alias_map = show_lookup_by_alias
    if resolved_alias_map is None:
        try:
            _, resolved_alias_map, _ = _build_show_lookup_maps(db)
        except Exception:  # noqa: BLE001
            resolved_alias_map = {}

    resolved_show_id: str | None = None
    for raw_name in (
        metadata_obj.get("imdb_fallback_show_name"),
        metadata_obj.get("show_name"),
        request_show_name,
    ):
        if not isinstance(raw_name, str) or not raw_name.strip():
            continue
        show_row = _find_show_row_by_alias(resolved_alias_map or {}, raw_name)
        if not isinstance(show_row, dict):
            continue
        resolved_show_id = _normalize_uuid_text(show_row.get("id"))
        if resolved_show_id:
            break

    if not resolved_show_id:
        return False
    if requested_show_id and resolved_show_id != requested_show_id:
        return False

    if show_exists_cache is None:
        show_exists_cache = {}
    if resolved_show_id in show_exists_cache:
        return bool(show_exists_cache[resolved_show_id])

    exists = False
    try:
        response = db.schema("core").table("shows").select("id").eq("id", resolved_show_id).limit(1).execute()
        exists = bool(getattr(response, "data", None))
    except Exception:  # noqa: BLE001
        exists = False
    show_exists_cache[resolved_show_id] = exists
    return exists


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
            db.schema("core").table("episodes").select("external_ids").eq("show_id", show_id_str).limit(5000).execute()
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
            db.schema("core").table("show_cast").select("person_id").eq("show_id", show_id_str).limit(5000).execute()
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
                db.schema("core").table("people").select("id,full_name,external_ids").in_("id", chunk).execute()
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
    if not isinstance(value, str) or not _is_http_url(value):
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


def _coerce_people_ids(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for entry in value:
        if not isinstance(entry, str):
            continue
        normalized = entry.strip()
        if normalized:
            out.append(normalized)
    return out


def _coerce_people_names(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for entry in value:
        if not isinstance(entry, str):
            continue
        normalized = entry.strip()
        if normalized:
            out.append(normalized)
    return out


def _build_tagged_people(people_ids: Any, people_names: Any) -> list[dict[str, str | None]]:
    ids = _coerce_people_ids(people_ids)
    names = _coerce_people_names(people_names)
    count = max(len(ids), len(names))
    out: list[dict[str, str | None]] = []
    for idx in range(count):
        person_id = ids[idx] if idx < len(ids) else None
        person_name = names[idx] if idx < len(names) else None
        if person_id is None and person_name is None:
            continue
        out.append({"person_id": person_id, "person_name": person_name})
    return out


def _normalize_person_id(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _person_name_key(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized or None


def _resolve_owner_person_name(
    *,
    owner_person_id: str | None,
    owner_person_name: str | None,
    tagged_people_ids: Any,
    tagged_people_names: Any,
) -> str | None:
    explicit_name = str(owner_person_name or "").strip()
    if explicit_name:
        return explicit_name
    owner_id = _normalize_person_id(owner_person_id)
    tagged_people = _build_tagged_people(tagged_people_ids, tagged_people_names)
    if owner_id:
        for tagged in tagged_people:
            tagged_id = _normalize_person_id(tagged.get("person_id"))
            tagged_name = str(tagged.get("person_name") or "").strip()
            if tagged_id == owner_id and tagged_name:
                return tagged_name
    if len(tagged_people) == 1:
        tagged_name = str(tagged_people[0].get("person_name") or "").strip()
        if tagged_name:
            return tagged_name
    return None


def _build_identity_candidate_person_ids(
    *,
    db: SupabaseAdminClient | None,
    allow_identity_assignment: bool,
    owner_person_id: str | None,
    tagged_people_ids: Any,
    tagged_people_names: Any = None,
    metadata_signals: list[Any] | None = None,
    person_name_id_cache: dict[str, str | None] | None = None,
) -> list[str]:
    return build_identity_candidate_person_ids_shared(
        db=db,
        allow_identity_assignment=allow_identity_assignment,
        owner_person_id=owner_person_id,
        tagged_people_ids=tagged_people_ids,
        tagged_people_names=tagged_people_names,
        metadata_signals=metadata_signals,
        person_name_id_cache=person_name_id_cache,
    )


def _resolve_person_name_by_id(
    db: SupabaseAdminClient | None,
    person_id: str | None,
    *,
    person_id_name_cache: dict[str, str | None] | None = None,
) -> str | None:
    normalized_person_id = _normalize_person_id(person_id)
    if not normalized_person_id:
        return None
    if person_id_name_cache is None:
        person_id_name_cache = {}
    if normalized_person_id in person_id_name_cache:
        return person_id_name_cache[normalized_person_id]
    if db is None:
        person_id_name_cache[normalized_person_id] = None
        return None

    resolved_name: str | None = None
    try:
        response = (
            db.schema("core").table("people").select("full_name").eq("id", normalized_person_id).limit(1).execute()
        )
        rows = response.data if isinstance(getattr(response, "data", None), list) else []
        if rows and isinstance(rows[0], dict):
            candidate = rows[0].get("full_name")
            if isinstance(candidate, str) and candidate.strip():
                resolved_name = candidate.strip()
    except Exception:  # noqa: BLE001
        resolved_name = None

    person_id_name_cache[normalized_person_id] = resolved_name
    return resolved_name


def _resolve_runtime_person_reference_pools(
    db: SupabaseAdminClient,
    *,
    candidate_person_ids: list[str] | None,
    request_show_id: UUID | None,
    request_show_name: str | None,
    reference_cache: dict[str, list[dict[str, Any]]],
    person_id_name_cache: dict[str, str | None] | None = None,
) -> list[dict[str, Any]]:
    pools: list[dict[str, Any]] = []
    if not candidate_person_ids:
        return pools
    for raw_person_id in candidate_person_ids:
        person_id = str(raw_person_id or "").strip()
        if not person_id:
            continue
        if person_id not in reference_cache:
            references: list[dict[str, Any]] = []
            try:
                profile = build_owner_tagging_reference_profile(
                    db,
                    person_id,
                    show_id=request_show_id,
                    show_name=request_show_name,
                )
                used_raw = profile.get("used")
                if isinstance(used_raw, list):
                    references = [entry for entry in used_raw if isinstance(entry, dict)]
                if references:
                    references = cast(list[dict[str, Any]], sync_owner_tagging_reference_usage(
                        db,
                        person_id,
                        used_references=references,
                    ))
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to resolve runtime tagging references person_id=%s error=%s",
                    person_id,
                    exc,
                )
                references = []
            reference_cache[person_id] = references
        references = reference_cache.get(person_id) or []
        if references:
            pool_payload: dict[str, Any] = {"person_id": person_id, "references": references}
            resolved_name = _resolve_person_name_by_id(
                db,
                person_id,
                person_id_name_cache=person_id_name_cache,
            )
            if isinstance(resolved_name, str) and resolved_name:
                pool_payload["person_name"] = resolved_name
            pools.append(pool_payload)
    return pools


def _promote_owner_similarity_assignment(
    boxes: list[dict[str, Any]],
    *,
    owner_person_id: str | None,
    owner_person_name: str | None,
    allow_identity_assignment: bool,
) -> None:
    if not allow_identity_assignment or not boxes:
        return
    owner_id = _normalize_person_id(owner_person_id)
    owner_name = str(owner_person_name or "").strip() or None
    owner_name_key = _person_name_key(owner_name)
    if not owner_id and not owner_name_key:
        return

    owner_indexes = []
    for idx, box in enumerate(boxes):
        matches_owner_id = owner_id and _normalize_person_id(box.get("person_id")) == owner_id
        matches_owner_name = owner_name_key and _person_name_key(box.get("person_name")) == owner_name_key
        if matches_owner_id or matches_owner_name:
            owner_indexes.append(idx)
    if len(owner_indexes) <= 1:
        return

    winner_idx = max(
        owner_indexes,
        key=lambda idx: (
            1 if str(boxes[idx].get("match_status") or "").strip().lower() == "matched" else 0,
            float(boxes[idx].get("match_similarity") or 0.0),
            float(boxes[idx].get("confidence") or 0.0),
            float(boxes[idx].get("width") or 0.0) * float(boxes[idx].get("height") or 0.0),
        ),
    )

    for idx in owner_indexes:
        box = boxes[idx]
        if idx == winner_idx:
            continue
        box.pop("person_id", None)
        box.pop("person_name", None)
        if box.get("label_source") in {"identity_match", "owner_similarity_seed"}:
            box["label_source"] = "generic"
            box.pop("label", None)

    winner = boxes[winner_idx]
    if owner_id:
        winner["person_id"] = owner_id
    if owner_name:
        winner["person_name"] = owner_name
        winner["label"] = owner_name
    winner["label_source"] = "owner_similarity_seed"


def _face_similarity_for_tagged_person(
    box: dict[str, Any],
    *,
    person_id: str | None,
    person_name_key: str | None,
) -> float | None:
    if not person_id and not person_name_key:
        return None
    best_similarity: float | None = None

    match_similarity = box.get("match_similarity")
    if isinstance(match_similarity, (int, float)):
        box_person_id = _normalize_person_id(box.get("person_id"))
        box_person_name_key = _person_name_key(box.get("person_name"))
        if (person_id and box_person_id == person_id) or (person_name_key and box_person_name_key == person_name_key):
            best_similarity = float(match_similarity)

    match_candidates = box.get("match_candidates")
    if isinstance(match_candidates, list):
        for candidate in match_candidates:
            if not isinstance(candidate, dict):
                continue
            similarity = candidate.get("similarity")
            if not isinstance(similarity, (int, float)):
                continue
            candidate_person_id = _normalize_person_id(candidate.get("person_id"))
            candidate_person_name_key = _person_name_key(candidate.get("person_name"))
            if not (
                (person_id and candidate_person_id == person_id)
                or (person_name_key and candidate_person_name_key == person_name_key)
            ):
                continue
            similarity_value = float(similarity)
            if best_similarity is None or similarity_value > best_similarity:
                best_similarity = similarity_value

    if best_similarity is None:
        return None
    return max(0.0, min(1.0, best_similarity))


def _tagged_person_has_similarity_evidence(
    tagged: dict[str, str | None],
    boxes: list[dict[str, Any]],
) -> bool:
    tagged_id = _normalize_person_id(tagged.get("person_id"))
    tagged_name_key = _person_name_key(tagged.get("person_name"))
    if not tagged_id and not tagged_name_key:
        return False
    for box in boxes:
        similarity = _face_similarity_for_tagged_person(
            box,
            person_id=tagged_id,
            person_name_key=tagged_name_key,
        )
        if similarity is None:
            continue
        if similarity > FACE_MATCH_SCORE_EVIDENCE_MIN:
            return True
    return False


def _apply_similarity_lead_assignments(
    boxes: list[dict[str, Any]],
    *,
    tagged_people_ids: Any,
    tagged_people_names: Any,
) -> None:
    if not boxes:
        return
    tagged_people = _build_tagged_people(tagged_people_ids, tagged_people_names)
    if not tagged_people:
        return

    claims: list[dict[str, Any]] = []
    for tagged in tagged_people:
        tagged_id = _normalize_person_id(tagged.get("person_id"))
        tagged_name = str(tagged.get("person_name") or "").strip() or None
        tagged_name_key = _person_name_key(tagged_name)
        if not tagged_id and not tagged_name_key:
            continue

        ranked_faces: list[tuple[int, float, float, float]] = []
        for index, box in enumerate(boxes):
            similarity = _face_similarity_for_tagged_person(
                box,
                person_id=tagged_id,
                person_name_key=tagged_name_key,
            )
            if similarity is None:
                continue
            ranked_faces.append(
                (
                    index,
                    similarity,
                    float(box.get("confidence") or 0.0),
                    float(box.get("width") or 0.0) * float(box.get("height") or 0.0),
                )
            )

        if not ranked_faces:
            continue
        ranked_faces.sort(key=lambda item: (item[1], item[2], item[3]), reverse=True)
        best_index, best_similarity, best_confidence, best_area = ranked_faces[0]
        second_similarity = ranked_faces[1][1] if len(ranked_faces) > 1 else 0.0
        if best_similarity < FACE_MATCH_CROSS_FACE_LEAD_MIN_SIMILARITY:
            continue
        if (best_similarity - second_similarity) < FACE_MATCH_CROSS_FACE_LEAD_MIN:
            continue
        claims.append(
            {
                "index": best_index,
                "person_id": tagged_id,
                "person_name": tagged_name,
                "person_name_key": tagged_name_key,
                "similarity": best_similarity,
                "confidence": best_confidence,
                "area": best_area,
            }
        )

    if not claims:
        return
    claims.sort(key=lambda item: (item["similarity"], item["confidence"], item["area"]), reverse=True)
    claimed_faces: set[int] = set()
    claimed_people: set[str] = set()

    for claim in claims:
        box_index = int(claim["index"])
        if box_index in claimed_faces:
            continue
        person_id = claim.get("person_id")
        person_name = claim.get("person_name")
        person_name_key = claim.get("person_name_key")
        person_key = str(person_id or f"name:{person_name_key or ''}").strip()
        if not person_key or person_key in claimed_people:
            continue

        box = boxes[box_index]
        existing_label_source = str(box.get("label_source") or "").strip().lower()
        existing_person_id = _normalize_person_id(box.get("person_id"))
        existing_person_name_key = _person_name_key(box.get("person_name"))
        same_person = bool(
            (person_id and existing_person_id == person_id)
            or (person_name_key and existing_person_name_key == person_name_key)
        )
        if existing_label_source in {"identity_match", "owner_similarity_seed", "lead_override"} and not same_person:
            continue

        # Skip if already correctly matched for the same person (don't overwrite direct match with lead_override)
        existing_match_status = str(box.get("match_status") or "").strip().lower()
        if (
            same_person
            and existing_match_status == "matched"
            and existing_label_source in {"identity_match", "owner_similarity_seed"}
        ):
            claimed_faces.add(box_index)
            claimed_people.add(person_key)
            continue

        if person_id:
            box["person_id"] = person_id
        if isinstance(person_name, str) and person_name:
            box["person_name"] = person_name
            box["label"] = person_name
        box["label_source"] = "lead_override"
        box["match_status"] = "matched"
        box["match_reason"] = "cross_face_lead_override"
        box["match_similarity"] = round(float(claim["similarity"]), 4)
        claimed_faces.add(box_index)
        claimed_people.add(person_key)


def _has_any_similarity_evidence(boxes: list[dict[str, Any]]) -> bool:
    for box in boxes:
        match_similarity = box.get("match_similarity")
        if isinstance(match_similarity, (int, float)) and float(match_similarity) > FACE_MATCH_SCORE_EVIDENCE_MIN:
            return True
        raw_candidates = box.get("match_candidates")
        if not isinstance(raw_candidates, list):
            continue
        for candidate in raw_candidates:
            if not isinstance(candidate, dict):
                continue
            similarity = candidate.get("similarity")
            if isinstance(similarity, (int, float)) and float(similarity) > FACE_MATCH_SCORE_EVIDENCE_MIN:
                return True
    return False


def _apply_owner_only_fallback_assignment(
    boxes: list[dict[str, Any]],
    *,
    owner_person_id: str | None = None,
    owner_person_name: str | None = None,
) -> bool:
    owner_id = _normalize_person_id(owner_person_id)
    owner_name = str(owner_person_name or "").strip() or None
    if not owner_id and not owner_name:
        return False
    protected_label_sources = {"identity_match", "owner_similarity_seed", "lead_override", "owner_fallback_map"}
    owner_name_key = _person_name_key(owner_name)

    for box in boxes:
        box_person_id = _normalize_person_id(box.get("person_id"))
        box_person_name_key = _person_name_key(box.get("person_name"))
        if (owner_id and box_person_id == owner_id) or (owner_name_key and box_person_name_key == owner_name_key):
            box["label_source"] = "owner_fallback_map"
            box["match_status"] = "matched"
            box["match_reason"] = "owner_fallback_map"
            return True

    candidate_indexes = [
        idx
        for idx, box in enumerate(boxes)
        if str(box.get("label_source") or "").strip().lower() not in protected_label_sources
    ]
    if not candidate_indexes:
        candidate_indexes = list(range(len(boxes)))
    if not candidate_indexes:
        return False

    best_index = max(
        candidate_indexes,
        key=lambda idx: (
            float(boxes[idx].get("confidence") or 0.0),
            float(boxes[idx].get("width") or 0.0) * float(boxes[idx].get("height") or 0.0),
            -float(boxes[idx].get("x") or 0.0),
        ),
    )
    best_box = boxes[best_index]
    if owner_id:
        best_box["person_id"] = owner_id
    if owner_name:
        best_box["person_name"] = owner_name
        best_box["label"] = owner_name
    best_box["label_source"] = "owner_fallback_map"
    best_box["match_status"] = "matched"
    best_box["match_reason"] = "owner_fallback_map"
    return True


def _apply_tagged_people_assignments(
    boxes: list[dict[str, Any]],
    *,
    tagged_people_ids: Any,
    tagged_people_names: Any,
    owner_person_id: str | None = None,
    owner_person_name: str | None = None,
) -> None:
    if not boxes:
        return
    if len(boxes) == 1 and not _has_any_similarity_evidence(boxes):
        if _apply_owner_only_fallback_assignment(
            boxes,
            owner_person_id=owner_person_id,
            owner_person_name=owner_person_name,
        ):
            return
    if len(boxes) > 1 and not _has_any_similarity_evidence(boxes):
        if _apply_owner_only_fallback_assignment(
            boxes,
            owner_person_id=owner_person_id,
            owner_person_name=owner_person_name,
        ):
            return
    tagged_people = _build_tagged_people(tagged_people_ids, tagged_people_names)
    if not tagged_people:
        if len(boxes) == 1:
            _apply_owner_only_fallback_assignment(
                boxes,
                owner_person_id=owner_person_id,
                owner_person_name=owner_person_name,
            )
        return

    remaining_tags = list(tagged_people)
    assigned_name_keys = {
        _person_name_key(box.get("person_name"))
        for box in boxes
        if isinstance(box.get("person_name"), str) and box.get("person_name")
    }
    assigned_ids = {
        str(box.get("person_id")).strip()
        for box in boxes
        if isinstance(box.get("person_id"), str) and str(box.get("person_id")).strip()
    }
    filtered_remaining: list[dict[str, str | None]] = []
    for tagged in remaining_tags:
        tagged_id = str(tagged.get("person_id") or "").strip()
        tagged_name_key = _person_name_key(tagged.get("person_name"))
        if tagged_id and tagged_id in assigned_ids:
            continue
        if tagged_name_key and tagged_name_key in assigned_name_keys:
            continue
        filtered_remaining.append(tagged)
    remaining_tags = filtered_remaining

    scored_remaining_tags = [
        tagged for tagged in remaining_tags if _tagged_person_has_similarity_evidence(tagged, boxes)
    ]
    if scored_remaining_tags:
        remaining_tags = [
            tagged for tagged in remaining_tags if not _tagged_person_has_similarity_evidence(tagged, boxes)
        ]

    protected_label_sources = {"identity_match", "owner_similarity_seed", "lead_override", "owner_fallback_map"}
    unassigned_indexes = [
        idx
        for idx, box in enumerate(boxes)
        if not (
            (isinstance(box.get("person_id"), str) and str(box.get("person_id")).strip())
            or (isinstance(box.get("person_name"), str) and str(box.get("person_name")).strip())
        )
        and str(box.get("label_source") or "").strip().lower() not in protected_label_sources
    ]
    if not unassigned_indexes or not remaining_tags:
        return

    # Best-effort mode: assign as many remaining tags as possible in stable left->right order.
    # Deterministic mapping is still preferred when cardinality matches exactly.
    if len(remaining_tags) > len(unassigned_indexes):
        return

    sorted_unassigned = sorted(
        unassigned_indexes,
        key=lambda idx: (
            float(boxes[idx].get("x") or 0.0),
            float(boxes[idx].get("y") or 0.0),
        ),
    )
    for idx, tagged in zip(sorted_unassigned, remaining_tags, strict=False):
        tagged_id = str(tagged.get("person_id") or "").strip() or None
        tagged_name = str(tagged.get("person_name") or "").strip() or None
        tagged_name_key = _person_name_key(tagged_name)
        if tagged_id:
            boxes[idx]["person_id"] = tagged_id
        if tagged_name:
            boxes[idx]["person_name"] = tagged_name
            boxes[idx]["label"] = tagged_name
        label_source = (
            "deterministic_tag_map" if len(remaining_tags) == len(sorted_unassigned) else "best_effort_tag_map"
        )
        boxes[idx]["label_source"] = label_source
        boxes[idx]["match_status"] = "matched"
        boxes[idx]["match_reason"] = label_source
        similarity = _face_similarity_for_tagged_person(
            boxes[idx],
            person_id=_normalize_person_id(tagged_id),
            person_name_key=tagged_name_key,
        )
        if similarity is not None:
            boxes[idx]["match_similarity"] = round(float(similarity), 4)


def _backfill_assigned_person_names(
    boxes: list[dict[str, Any]],
    *,
    person_name_lookup_by_id: dict[str, str],
) -> None:
    if not boxes or not person_name_lookup_by_id:
        return
    for box in boxes:
        person_id = _normalize_person_id(box.get("person_id"))
        if not person_id:
            continue
        has_person_name = isinstance(box.get("person_name"), str) and str(box.get("person_name")).strip()
        if has_person_name:
            continue
        resolved_name = person_name_lookup_by_id.get(person_id)
        if not isinstance(resolved_name, str) or not resolved_name:
            continue
        box["person_name"] = resolved_name
        box["label"] = resolved_name


def _extract_square_crop_bbox(square_crop_bbox_raw: Any) -> list[float] | None:
    if isinstance(square_crop_bbox_raw, list) and len(square_crop_bbox_raw) >= 4:
        try:
            sx1 = _normalize_face_coord(float(square_crop_bbox_raw[0]))
            sy1 = _normalize_face_coord(float(square_crop_bbox_raw[1]))
            sx2 = _normalize_face_coord(float(square_crop_bbox_raw[2]))
            sy2 = _normalize_face_coord(float(square_crop_bbox_raw[3]))
            if sx2 > sx1 and sy2 > sy1:
                return [sx1, sy1, sx2, sy2]
        except (TypeError, ValueError):
            return None
    return None


def _extract_detection_boxes(result: Any, *, kind: str) -> list[dict[str, Any]]:
    detections = getattr(result, "detections", None) or []
    boxes: list[dict[str, Any]] = []
    for det in detections:
        det_kind = str(getattr(det, "kind", "face")).strip().lower()
        if det_kind != kind:
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
        match_reason_raw = getattr(det, "match_reason", None)
        match_candidates_raw = getattr(det, "match_candidates", None)
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
        match_reason = (
            str(match_reason_raw).strip().lower()
            if isinstance(match_reason_raw, str) and match_reason_raw.strip()
            else None
        )
        # Ignore tiny background detections (e.g., shelf dolls) from downstream tagging/crop logic.
        if kind == "face" and match_reason == "face_too_small":
            continue
        match_candidates: list[dict[str, Any]] = []
        if isinstance(match_candidates_raw, list):
            for candidate in match_candidates_raw:
                if not isinstance(candidate, dict):
                    continue
                similarity_raw = candidate.get("similarity")
                if not isinstance(similarity_raw, (int, float)):
                    continue
                normalized_candidate: dict[str, Any] = {
                    "similarity": round(max(0.0, min(1.0, float(similarity_raw))), 4)
                }
                person_id_candidate = candidate.get("person_id")
                if isinstance(person_id_candidate, str) and person_id_candidate.strip():
                    normalized_candidate["person_id"] = person_id_candidate.strip()
                person_name_candidate = candidate.get("person_name")
                if isinstance(person_name_candidate, str) and person_name_candidate.strip():
                    normalized_candidate["person_name"] = person_name_candidate.strip()
                match_candidates.append(normalized_candidate)
        square_crop_bbox = _extract_square_crop_bbox(square_crop_bbox_raw)
        boxes.append(
            {
                "kind": kind,
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
                **({"match_reason": match_reason} if match_reason else {}),
                **({"match_candidates": match_candidates} if match_candidates else {}),
                **({"square_crop_bbox": square_crop_bbox} if square_crop_bbox else {}),
            }
        )
    return boxes


def _build_detection_boxes(
    result: Any,
    *,
    tagged_people_ids: Any = None,
    tagged_people_names: Any = None,
    owner_person_id: str | None = None,
    owner_person_name: str | None = None,
    allow_identity_assignment: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    face_boxes = _extract_detection_boxes(result, kind="face")
    diagnostics = _empty_auto_count_diagnostics()
    diagnostics["auto_faces_detected"] = len(face_boxes)
    explicit_owner_name = str(owner_person_name or "").strip() or None
    tagged_people_lookup = _build_tagged_people(tagged_people_ids, tagged_people_names)
    person_name_lookup_by_id: dict[str, str] = {}
    for tagged in tagged_people_lookup:
        tagged_id = _normalize_person_id(tagged.get("person_id"))
        tagged_name = str(tagged.get("person_name") or "").strip()
        if tagged_id and tagged_name and tagged_id not in person_name_lookup_by_id:
            person_name_lookup_by_id[tagged_id] = tagged_name
    normalized_owner_id = _normalize_person_id(owner_person_id)
    normalized_owner_name = explicit_owner_name or ""
    if normalized_owner_id and normalized_owner_name and normalized_owner_id not in person_name_lookup_by_id:
        person_name_lookup_by_id[normalized_owner_id] = normalized_owner_name

    if face_boxes:
        boxes: list[dict[str, Any]] = []
        for idx, box in enumerate(face_boxes, start=1):
            if allow_identity_assignment:
                box_person_id = _normalize_person_id(box.get("person_id"))
                if box_person_id and not str(box.get("person_name") or "").strip():
                    resolved_name = person_name_lookup_by_id.get(box_person_id)
                    if isinstance(resolved_name, str) and resolved_name:
                        box["person_name"] = resolved_name
                        if not str(box.get("label") or "").strip():
                            box["label"] = resolved_name
                if isinstance(box.get("match_candidates"), list) and person_name_lookup_by_id:
                    enriched_candidates: list[dict[str, Any]] = []
                    for raw_candidate in box.get("match_candidates") or []:
                        if not isinstance(raw_candidate, dict):
                            continue
                        candidate = dict(raw_candidate)
                        candidate_person_id = _normalize_person_id(candidate.get("person_id"))
                        has_name = bool(
                            isinstance(candidate.get("person_name"), str) and str(candidate.get("person_name")).strip()
                        )
                        if candidate_person_id and not has_name:
                            resolved_candidate_name = person_name_lookup_by_id.get(candidate_person_id)
                            if isinstance(resolved_candidate_name, str) and resolved_candidate_name:
                                candidate["person_name"] = resolved_candidate_name
                        enriched_candidates.append(candidate)
                    box["match_candidates"] = enriched_candidates
            has_identity = allow_identity_assignment and bool(box.get("person_id") or box.get("person_name"))
            entry = {
                "index": idx,
                "kind": "face",
                "x": box["x"],
                "y": box["y"],
                "width": box["width"],
                "height": box["height"],
                "confidence": box.get("confidence"),
                "source_kind": "face",
                "label_source": "identity_match" if has_identity else "generic",
                **({"person_id": box.get("person_id")} if allow_identity_assignment and box.get("person_id") else {}),
                **(
                    {"person_name": box.get("person_name")}
                    if allow_identity_assignment and box.get("person_name")
                    else {}
                ),
                **({"label": box.get("label")} if allow_identity_assignment and box.get("label") else {}),
                **(
                    {"match_similarity": box.get("match_similarity")}
                    if allow_identity_assignment and box.get("match_similarity") is not None
                    else {}
                ),
                **(
                    {"match_status": box.get("match_status")}
                    if allow_identity_assignment and box.get("match_status")
                    else {}
                ),
                **(
                    {"match_reason": box.get("match_reason")}
                    if allow_identity_assignment and box.get("match_reason")
                    else {}
                ),
                **(
                    {"match_candidates": box.get("match_candidates")}
                    if allow_identity_assignment and isinstance(box.get("match_candidates"), list)
                    else {}
                ),
                **({"square_crop_bbox": box.get("square_crop_bbox")} if box.get("square_crop_bbox") else {}),
            }
            boxes.append(entry)
        if allow_identity_assignment:
            resolved_owner_name = _resolve_owner_person_name(
                owner_person_id=owner_person_id,
                owner_person_name=owner_person_name,
                tagged_people_ids=tagged_people_ids,
                tagged_people_names=tagged_people_names,
            )
            owner_name_for_assignment = resolved_owner_name if (normalized_owner_id or explicit_owner_name) else None
            _promote_owner_similarity_assignment(
                boxes,
                owner_person_id=owner_person_id,
                owner_person_name=owner_name_for_assignment,
                allow_identity_assignment=allow_identity_assignment,
            )
            _apply_similarity_lead_assignments(
                boxes,
                tagged_people_ids=tagged_people_ids,
                tagged_people_names=tagged_people_names,
            )
            _apply_tagged_people_assignments(
                boxes,
                tagged_people_ids=tagged_people_ids,
                tagged_people_names=tagged_people_names,
                owner_person_id=owner_person_id,
                owner_person_name=owner_name_for_assignment,
            )
            _backfill_assigned_person_names(
                boxes,
                person_name_lookup_by_id=person_name_lookup_by_id,
            )
        return boxes, diagnostics

    if int(getattr(result, "people_count", 0) or 0) > 0:
        diagnostics["auto_no_face_rows"] = 1

    person_boxes = _extract_detection_boxes(result, kind="person")
    if not person_boxes:
        return [], diagnostics

    def _person_box_sort_key(box: dict[str, Any]) -> tuple[float, float, float]:
        conf = box.get("confidence")
        return (
            -(conf if isinstance(conf, (int, float)) else 0.0),
            float(box.get("x") or 0.0),
            float(box.get("y") or 0.0),
        )

    person_boxes = sorted(person_boxes, key=_person_box_sort_key)

    tagged_people = _build_tagged_people(tagged_people_ids, tagged_people_names)
    deterministic_assignments: dict[int, dict[str, str | None]] = {}
    if allow_identity_assignment and len(tagged_people) == 1:
        deterministic_assignments[0] = tagged_people[0]
    elif allow_identity_assignment and len(tagged_people) == len(person_boxes):
        for idx, tagged in enumerate(tagged_people):
            deterministic_assignments[idx] = tagged

    boxes: list[dict[str, Any]] = []
    for idx, box in enumerate(person_boxes, start=1):
        assignment = deterministic_assignments.get(idx - 1)
        assigned_person_id = assignment.get("person_id") if assignment else None
        assigned_person_name = assignment.get("person_name") if assignment else None
        fallback_label = str(box.get("label") or "").strip() or f"Person {idx}"
        label = assigned_person_name or fallback_label
        label_source = "deterministic_tag_map" if assignment else "generic"
        entry = {
            "index": idx,
            "kind": "face",
            "x": box["x"],
            "y": box["y"],
            "width": box["width"],
            "height": box["height"],
            "confidence": box.get("confidence"),
            "source_kind": "person_fallback",
            "label_source": label_source,
            "fallback_reason": "no_faces_detected",
            "label": label,
            **({"person_id": assigned_person_id} if assigned_person_id else {}),
            **({"person_name": assigned_person_name} if assigned_person_name else {}),
            **({"match_status": "matched"} if assignment else {}),
            **({"match_reason": "deterministic_tag_map"} if assignment else {}),
            **({"square_crop_bbox": box.get("square_crop_bbox")} if box.get("square_crop_bbox") else {}),
        }
        boxes.append(entry)
    return boxes, diagnostics


def _count_face_crop_sources(face_boxes: list[dict[str, Any]], face_crops: list[dict[str, Any]]) -> tuple[int, int]:
    if not face_crops:
        return (0, 0)
    source_by_index: dict[int, str] = {}
    for box in face_boxes:
        index = box.get("index")
        if isinstance(index, int):
            source_by_index[index] = str(box.get("source_kind") or "face")
    face_crops_generated = 0
    person_fallback_crops_generated = 0
    for crop in face_crops:
        index = crop.get("index")
        source_kind = source_by_index.get(int(index)) if isinstance(index, int) else "face"
        if source_kind == "person_fallback":
            person_fallback_crops_generated += 1
        else:
            face_crops_generated += 1
    return face_crops_generated, person_fallback_crops_generated


def _face_boxes_signature(face_boxes: Any) -> str:
    if not isinstance(face_boxes, list):
        return ""
    normalized: list[dict[str, Any]] = []
    for entry in face_boxes:
        if not isinstance(entry, dict):
            continue
        normalized.append(
            {
                "index": entry.get("index"),
                "x": round(float(entry.get("x") or 0.0), 5),
                "y": round(float(entry.get("y") or 0.0), 5),
                "width": round(float(entry.get("width") or 0.0), 5),
                "height": round(float(entry.get("height") or 0.0), 5),
                "source_kind": str(entry.get("source_kind") or "face"),
                "person_id": str(entry.get("person_id") or ""),
                "person_name": str(entry.get("person_name") or ""),
            }
        )
    if not normalized:
        return ""
    normalized.sort(key=lambda item: int(item.get("index") or 0))
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


def _can_reuse_face_crop_cache(
    *,
    previous_face_boxes: Any,
    previous_face_crops: Any,
    next_face_boxes: list[dict[str, Any]],
) -> bool:
    if not isinstance(previous_face_crops, list) or not previous_face_crops:
        return False
    for crop in previous_face_crops:
        if not isinstance(crop, dict):
            return False
        crop_url = str(crop.get("crop_url") or crop.get("url") or "").strip()
        if not crop_url:
            return False
    return _face_boxes_signature(previous_face_boxes) == _face_boxes_signature(next_face_boxes)


def _build_face_boxes(result: Any) -> list[dict[str, Any]]:
    boxes, _ = _build_detection_boxes(result)
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
    if strategy not in {"face_torso_v2", "owner_face_box_v1"}:
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
    raw_metadata = row.get("metadata")
    metadata: dict[str, Any] = raw_metadata if isinstance(raw_metadata, dict) else {}
    page_url_val = metadata.get("page_url")
    source_page_url_val = metadata.get("source_page_url")
    source_page_url = (
        page_url_val
        if isinstance(page_url_val, str)
        else source_page_url_val
        if isinstance(source_page_url_val, str)
        else None
    )

    if source in {"fandom", "fandom-gallery"} and isinstance(source_url, str):
        normalized = normalize_fandom_file_url(source_url, referer=source_page_url)
        return _iter_unique_urls([hosted_url, normalized, source_url])
    return _iter_unique_urls([hosted_url, source_url])


def _fetch_person_media_link_rows(
    db: SupabaseAdminClient,
    person_id: str,
    *,
    link_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    normalized_link_ids = _normalize_scope_ids(link_ids)
    if link_ids is not None and not normalized_link_ids:
        return []
    links_resp = (
        db.schema("core")
        .table("media_links")
        .select("id, media_asset_id, context")
        .eq("entity_type", "person")
        .eq("entity_id", person_id)
        .eq("kind", "gallery")
    )
    if normalized_link_ids:
        links_resp = links_resp.in_("id", normalized_link_ids)
    links_resp = links_resp.execute()
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
            "hosted_content_type, hosted_bytes, hosted_etag, width, height, caption, metadata, "
            "ingest_status, ingest_last_error"
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
                "caption": asset.get("caption"),
                "metadata": asset.get("metadata") if isinstance(asset.get("metadata"), dict) else {},
                "ingest_status": asset.get("ingest_status"),
                "ingest_last_error": asset.get("ingest_last_error"),
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


def _owner_face_crop_payload(
    face_boxes: list[dict[str, Any]],
    *,
    owner_person_id: str | None = None,
    owner_person_name: str | None = None,
) -> dict[str, Any] | None:
    if not face_boxes:
        return None
    owner_id = str(owner_person_id or "").strip()
    owner_name_key = _person_name_key(owner_person_name)

    candidates: list[dict[str, Any]] = []
    for box in face_boxes:
        box_person_id = str(box.get("person_id") or "").strip()
        box_person_name_key = _person_name_key(box.get("person_name"))
        if owner_id and box_person_id == owner_id:
            candidates.append(box)
            continue
        if owner_name_key and box_person_name_key == owner_name_key:
            candidates.append(box)

    if not candidates:
        return None

    min_similarity = _owner_face_match_similarity_min()
    qualified_candidates: list[dict[str, Any]] = []
    for box in candidates:
        match_status = str(box.get("match_status") or "").strip().lower()
        match_similarity = box.get("match_similarity")
        if match_status != "matched":
            continue
        if not isinstance(match_similarity, (int, float)):
            continue
        if float(match_similarity) < min_similarity:
            continue
        qualified_candidates.append(box)
    if not qualified_candidates:
        return None

    best = max(
        qualified_candidates,
        key=lambda item: (
            float(item.get("match_similarity") or 0.0),
            float(item.get("confidence") or 0.0),
            float(item.get("width") or 0.0) * float(item.get("height") or 0.0),
        ),
    )
    # Prefer square_crop_bbox from vision API (includes proper padding)
    cx = 0.5
    cy = 0.5
    target_span = 0.5
    scb = best.get("square_crop_bbox")
    if isinstance(scb, list) and len(scb) >= 4:
        try:
            scb_x1, scb_y1, scb_x2, scb_y2 = [float(v) for v in scb[:4]]
            scb_height = max(scb_y2 - scb_y1, 1e-4)
            cx = max(0.0, min(1.0, (scb_x1 + scb_x2) / 2.0))
            cy = max(0.0, min(1.0, scb_y1 + (scb_height * 0.45)))
            target_span = max(0.34, min(0.72, scb_height * 1.5))
        except (TypeError, ValueError):
            scb = None

    if not (isinstance(scb, list) and len(scb) >= 4):
        x = float(best.get("x") or 0.0)
        y = float(best.get("y") or 0.0)
        width = max(float(best.get("width") or 0.0), 1e-4)
        height = max(float(best.get("height") or 0.0), 1e-4)
        cx = max(0.0, min(1.0, x + (width / 2.0)))
        cy = max(0.0, min(1.0, y + (height * 0.62)))
        target_span = max(0.34, min(0.72, height * 2.8))

    zoom = max(1.05, min(1.9, 0.8 / target_span))
    return {
        "x": round(cx * 100.0, 1),
        "y": round(cy * 100.0, 1),
        "zoom": round(zoom, 2),
        "mode": "auto",
        "strategy": "owner_face_box_v1",
        "generated_at": datetime.now(UTC).isoformat(),
    }


def _recenter_person_gallery_images(
    db: SupabaseAdminClient,
    person_id: str,
    sources: list[SourceType],
    *,
    photo_ids: list[str] | None = None,
    media_link_ids: list[str] | None = None,
    progress_cb: Callable[[int, int], None] | None = None,
    force: bool = False,
    max_parallelism: int | None = None,
    owner_person_name: str | None = None,
    owner_reference_images: list[dict[str, Any]] | None = None,
    prefer_fast_pass: bool = True,
) -> tuple[int, int, int, int]:
    attempted = 0
    succeeded = 0
    failed = 0
    skipped_manual = 0

    candidate_sources = [s for s in sources if s in ALL_SOURCES]
    if not candidate_sources:
        return attempted, succeeded, failed, skipped_manual
    normalized_photo_ids = _normalize_scope_ids(photo_ids)
    normalized_media_link_ids = _normalize_scope_ids(media_link_ids)
    if photo_ids is not None and not normalized_photo_ids:
        cast_rows: list[dict[str, Any]] = []
    else:
        cast_rows = []
    if media_link_ids is not None and not normalized_media_link_ids:
        media_rows: list[dict[str, Any]] = []
    else:
        media_rows = []

    try:
        from trr_backend.clients.screenalytics import (
            ScreenalyticsClientError,
            count_people,
            is_screenalytics_configured,
        )

        if not is_screenalytics_configured():
            return attempted, succeeded, failed, skipped_manual

        resolved_owner_name = owner_person_name
        if not (isinstance(resolved_owner_name, str) and resolved_owner_name.strip()):
            person_row = _get_person_details(db, person_id)
            if isinstance(person_row, dict):
                name_raw = person_row.get("full_name")
                if isinstance(name_raw, str) and name_raw.strip():
                    resolved_owner_name = name_raw.strip()

        resolved_owner_reference_images: list[dict[str, object]] = (
            [entry for entry in owner_reference_images if isinstance(entry, dict)]
            if isinstance(owner_reference_images, list)
            else []
        )
        if not resolved_owner_reference_images:
            try:
                owner_reference_profile = build_owner_tagging_reference_profile(
                    db,
                    person_id,
                    show_id=None,
                    show_name=None,
                )
                raw_refs = owner_reference_profile.get("used")
                if isinstance(raw_refs, list):
                    resolved_owner_reference_images = [entry for entry in raw_refs if isinstance(entry, dict)]
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "Centering owner reference profile unavailable person_id=%s error=%s",
                    person_id,
                    exc,
                )

        if not (photo_ids is not None and not normalized_photo_ids):
            cast_query = (
                db.schema("core")
                .table("cast_photos")
                .select("id, hosted_url, url, image_url, thumb_url, source_page_url, source, metadata")
                .eq("person_id", person_id)
                .in_("source", candidate_sources)
            )
            if normalized_photo_ids:
                cast_query = cast_query.in_("id", normalized_photo_ids)
            cast_rows = cast_query.execute().data or []

        if not (media_link_ids is not None and not normalized_media_link_ids):
            media_rows = _fetch_person_media_link_rows(
                db,
                person_id,
                link_ids=normalized_media_link_ids,
            )

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
        if total <= 0:
            return attempted, succeeded, failed, skipped_manual
        attempted += total

        resolved_max_parallelism = (
            max(1, int(max_parallelism))
            if isinstance(max_parallelism, int) and max_parallelism > 0
            else _read_positive_int_env("CROP_MAX_PARALLEL", 8)
        )

        def _process_entry(entry: dict[str, Any]) -> tuple[bool, str | None]:
            result = None
            last_error: ScreenalyticsClientError | None = None
            for image_url in entry["urls"]:
                try:
                    result = count_people(
                        image_url,
                        candidate_person_ids=[person_id],
                        owner_person_id=person_id,
                        owner_reference_images=resolved_owner_reference_images or None,
                        prefer_fast_pass=bool(prefer_fast_pass),
                    )
                    break
                except TypeError:
                    try:
                        result = count_people(
                            image_url,
                            candidate_person_ids=[person_id],
                            owner_person_id=person_id,
                            owner_reference_images=resolved_owner_reference_images or None,
                        )
                        break
                    except TypeError:
                        result = count_people(image_url, candidate_person_ids=[person_id])
                        break
                except ScreenalyticsClientError as exc:
                    last_error = exc
            try:
                if result is None:
                    raise last_error or ScreenalyticsClientError("Unable to center/crop image")
                owner_crop_payload = _owner_face_crop_payload(
                    _extract_detection_boxes(result, kind="face"),
                    owner_person_id=person_id,
                    owner_person_name=resolved_owner_name,
                )
                crop_payload = owner_crop_payload or _apply_auto_crop_payload(result)
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
                logger.info(
                    "Centering crop saved origin=%s id=%s strategy=%s x=%.1f y=%.1f zoom=%.2f",
                    entry["origin"],
                    entry["id"],
                    str(crop_payload.get("strategy") or "unknown"),
                    float(crop_payload.get("x", 50)),
                    float(crop_payload.get("y", 32)),
                    float(crop_payload.get("zoom", 1)),
                )
                return True, None
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Centering crop failed origin=%s id=%s error=%s",
                    entry["origin"],
                    entry["id"],
                    exc,
                )
                return False, str(exc)

        done = 0
        if resolved_max_parallelism <= 1:
            for entry in to_process:
                entry_succeeded, _error = _process_entry(entry)
                if entry_succeeded:
                    succeeded += 1
                else:
                    failed += 1
                done += 1
                if progress_cb:
                    progress_cb(done, total)
        else:
            chunk_size = max(1, resolved_max_parallelism * 4)
            for chunk in _chunked(to_process, chunk_size):
                with ThreadPoolExecutor(max_workers=resolved_max_parallelism) as executor:
                    future_map = {executor.submit(_process_entry, entry): entry for entry in chunk}
                    for future in as_completed(future_map):
                        try:
                            entry_succeeded, _error = future.result()
                        except Exception as exc:  # noqa: BLE001
                            entry_succeeded = False
                            _error = str(exc)
                        if entry_succeeded:
                            succeeded += 1
                        else:
                            failed += 1
                        done += 1
                        if progress_cb:
                            progress_cb(done, total)
    except Exception as exc:
        logger.exception("Centering/cropping setup failed for %s: %s", person_id, exc)

    return attempted, succeeded, failed, skipped_manual


def _mirror_person_photos(
    db: SupabaseAdminClient,
    person_id: str,
    imdb_person_id: str | None,
    *,
    force: bool = False,
    max_parallelism: int = 12,
    batch_size: int = 200,
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
    if not force:
        deduped_rows: list[dict[str, Any]] = []
        seen_url_keys: set[str] = set()
        for row in rows:
            url_key = (
                str(
                    row.get("hosted_url")
                    or row.get("image_url")
                    or row.get("url")
                    or row.get("thumb_url")
                    or row.get("source_page_url")
                    or row.get("id")
                    or ""
                )
                .strip()
                .lower()
            )
            if not url_key:
                continue
            if url_key in seen_url_keys:
                continue
            seen_url_keys.add(url_key)
            deduped_rows.append(row)
        rows = deduped_rows

    mirrored, failed = 0, 0
    total_rows = len(rows)
    done = 0
    max_workers = max(1, int(max_parallelism))
    chunks = _chunked(rows, max(1, int(batch_size)))

    for chunk in chunks:
        work_items: list[tuple[dict[str, Any], str]] = []
        for row in chunk:
            local_row = dict(row)
            if not local_row.get("imdb_person_id") and imdb_person_id:
                local_row["imdb_person_id"] = imdb_person_id
            work_items.append((local_row, str(local_row.get("id") or "")))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(mirror_cast_photo_row, row, force=force): (row, row_id) for row, row_id in work_items
            }
            for future in as_completed(future_map):
                row, row_id = future_map[future]
                try:
                    patch = future.result()
                    if patch and row_id:
                        update_cast_photo_hosted_fields(db, row_id, patch)
                        mirrored += 1
                except Exception as exc:
                    logger.warning("Mirror failed for %s: %s", row.get("id"), exc)
                    failed += 1
                done += 1
                if progress_cb:
                    progress_cb(done, total_rows)
    return mirrored, failed


def _mirror_person_media_assets(
    db: SupabaseAdminClient,
    person_id: str,
    *,
    force: bool = False,
    max_parallelism: int = 12,
    batch_size: int = 200,
    progress_cb: Callable[[int, int], None] | None = None,
) -> tuple[int, int]:
    from trr_backend.media.s3_mirror import mirror_media_asset_row
    from trr_backend.repositories.media_assets import (
        update_asset_with_hosted_fields,
        update_asset_with_mirror_result,
        update_ingest_status,
    )

    def _is_duplicate_media_asset_hash_error(exc: Exception) -> bool:
        message = str(exc)
        return (
            "media_assets_source_hosted_sha_uq" in message
            or "media_assets_sha256_unique" in message
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

    if not force:
        filtered_assets: list[dict[str, Any]] = []
        for row in unique_assets:
            if str(row.get("ingest_status") or "").strip().lower() == "failed":
                logger.info(
                    "Skipping previously failed media asset during ingest-only mirror run asset_id=%s error=%s",
                    row.get("media_asset_id"),
                    row.get("ingest_last_error"),
                )
                continue
            filtered_assets.append(row)
        unique_assets = filtered_assets
        if not unique_assets:
            return 0, 0

    mirrored = 0
    failed = 0
    total_rows = len(unique_assets)
    done = 0
    max_workers = max(1, int(max_parallelism))
    chunks = _chunked(unique_assets, max(1, int(batch_size)))

    for chunk in chunks:
        work_items: list[tuple[dict[str, Any], str]] = []
        for row in chunk:
            asset_id = str(row.get("media_asset_id") or "")
            if not asset_id:
                failed += 1
                done += 1
                if progress_cb:
                    progress_cb(done, total_rows)
                continue
            try:
                update_ingest_status(db, asset_id, "in_progress")
            except Exception:  # noqa: BLE001
                pass
            work_items.append((dict(row), asset_id))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(mirror_media_asset_row, row, force=force): (row, asset_id)
                for row, asset_id in work_items
            }
            for future in as_completed(future_map):
                row, asset_id = future_map[future]
                try:
                    patch = future.result()
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
                            try:
                                update_asset_with_mirror_result(
                                    db,
                                    asset_id=asset_id,
                                    sha256=str(patch.get("sha256") or patch.get("hosted_sha256") or ""),
                                    hosted_bucket=str(patch.get("hosted_bucket") or ""),
                                    hosted_key=str(patch.get("hosted_key") or ""),
                                    hosted_url=str(patch.get("hosted_url") or ""),
                                    hosted_bytes=int(patch.get("hosted_bytes") or 0),
                                    hosted_content_type=(
                                        str(_ct) if (_ct := patch.get("hosted_content_type")) is not None else None
                                    ),
                                    hosted_etag=(
                                        str(_et) if (_et := patch.get("hosted_etag")) is not None else None
                                    ),
                                    width=int(_w) if (_w := patch.get("width")) is not None else None,
                                    height=int(_h) if (_h := patch.get("height")) is not None else None,
                                    completed_at=completed_at,
                                    metadata=_m if isinstance((_m := patch.get("metadata")), dict) else None,
                                )
                            except Exception as exc:
                                if not _is_duplicate_media_asset_hash_error(exc):
                                    raise
                                logger.info(
                                    "Mirror dedupe fallback for media asset %s after duplicate hash conflict: %s",
                                    asset_id,
                                    exc,
                                )
                                update_asset_with_hosted_fields(
                                    db,
                                    asset_id=asset_id,
                                    hosted_bucket=str(patch.get("hosted_bucket") or ""),
                                    hosted_key=str(patch.get("hosted_key") or ""),
                                    hosted_url=str(patch.get("hosted_url") or ""),
                                    hosted_bytes=int(patch.get("hosted_bytes") or 0),
                                    hosted_content_type=(
                                        str(_ct2)
                                        if (_ct2 := patch.get("hosted_content_type")) is not None
                                        else None
                                    ),
                                    hosted_etag=(
                                        str(_et2)
                                        if (_et2 := patch.get("hosted_etag")) is not None
                                        else None
                                    ),
                                    width=int(_w2) if (_w2 := patch.get("width")) is not None else None,
                                    height=int(_h2) if (_h2 := patch.get("height")) is not None else None,
                                    completed_at=completed_at,
                                    metadata=_m2 if isinstance((_m2 := patch.get("metadata")), dict) else None,
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
                done += 1
                if progress_cb:
                    progress_cb(done, total_rows)

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
    owner_person_name: str | None = None,
    owner_reference_images: list[dict[str, Any]] | None = None,
    owner_reference_sync_cb: Callable[[list[dict[str, Any]]], None] | None = None,
    photo_ids: list[str] | None = None,
    force_recount: bool = False,
    request_show_id: UUID | None = None,
    request_show_name: str | None = None,
    progress_cb: Callable[[int, int], None] | None = None,
    diagnostics: dict[str, int] | None = None,
    stage_stats: dict[str, int] | None = None,
    failed_photo_ids: list[str] | None = None,
    tagging_batch_size: int = 32,
    prefer_fast_pass: bool = True,
) -> tuple[int, int, int]:
    """Auto-count people for selected cast photos. Returns (attempted, succeeded, failed)."""
    auto_counts_attempted = 0
    auto_counts_succeeded = 0
    auto_counts_failed = 0
    diagnostics_local = _empty_auto_count_diagnostics()

    candidate_sources = [s for s in sources if s in ALL_SOURCES]
    if not candidate_sources:
        if diagnostics is not None:
            _merge_counter_fields(diagnostics, diagnostics_local, AUTO_COUNT_DIAGNOSTIC_FIELDS)
        return auto_counts_attempted, auto_counts_succeeded, auto_counts_failed
    normalized_photo_ids = _normalize_scope_ids(photo_ids)
    if photo_ids is not None and not normalized_photo_ids:
        _record_stage_row_stats(stage_stats, attempted_rows=0, skipped_existing_rows=0)
        if diagnostics is not None:
            _merge_counter_fields(diagnostics, diagnostics_local, AUTO_COUNT_DIAGNOSTIC_FIELDS)
        return auto_counts_attempted, auto_counts_succeeded, auto_counts_failed

    try:
        from trr_backend.clients.screenalytics import (
            ScreenalyticsClientError,
            ScreenalyticsUnavailableError,
            count_people,
            count_people_batch,
            get_screenalytics_unavailable_state,
            is_screenalytics_configured,
        )
        from trr_backend.repositories.cast_photo_tags import (
            get_tags_by_photo_ids,
            has_manual_tags,
            upsert_cast_photo_tags,
        )

        if not is_screenalytics_configured():
            if diagnostics is not None:
                _merge_counter_fields(diagnostics, diagnostics_local, AUTO_COUNT_DIAGNOSTIC_FIELDS)
            return auto_counts_attempted, auto_counts_succeeded, auto_counts_failed
        unavailable, _, _ = get_screenalytics_unavailable_state()
        if unavailable:
            if diagnostics is not None:
                _merge_counter_fields(diagnostics, diagnostics_local, AUTO_COUNT_DIAGNOSTIC_FIELDS)
            return auto_counts_attempted, auto_counts_succeeded, auto_counts_failed

        query = (
            db.schema("core")
            .table("cast_photos")
            .select(
                "id, hosted_url, hosted_content_type, url, image_url, thumb_url, "
                "source_page_url, people_names, title_names, caption, source, metadata"
            )
            .eq("person_id", person_id)
            .in_("source", candidate_sources)
        )
        if normalized_photo_ids:
            query = query.in_("id", normalized_photo_ids)
        response = query.execute()

        if hasattr(response, "error") and response.error:
            logger.warning("Auto-count query failed for %s: %s", person_id, response.error)
            if diagnostics is not None:
                _merge_counter_fields(diagnostics, diagnostics_local, AUTO_COUNT_DIAGNOSTIC_FIELDS)
            return auto_counts_attempted, auto_counts_succeeded, auto_counts_failed

        rows = response.data or []
        if not rows:
            if diagnostics is not None:
                _merge_counter_fields(diagnostics, diagnostics_local, AUTO_COUNT_DIAGNOSTIC_FIELDS)
            return auto_counts_attempted, auto_counts_succeeded, auto_counts_failed

        tag_rows = get_tags_by_photo_ids(db, [str(row["id"]) for row in rows])
        show_lookup_by_alias: dict[str, dict[str, Any]] | None = None
        try:
            _, show_lookup_by_alias, _ = _build_show_lookup_maps(db)
        except Exception:  # noqa: BLE001
            show_lookup_by_alias = {}
        show_exists_cache: dict[str, bool] = {}
        show_name_cache: dict[str, str | None] = {}
        person_name_id_cache: dict[str, str | None] = {}
        person_id_name_cache: dict[str, str | None] = {}
        reference_pool_cache: dict[str, list[dict[str, Any]]] = {}
        to_process: list[dict[str, Any]] = []
        skipped_existing_rows = 0
        for row in rows:
            tag_row = tag_rows.get(str(row["id"]))
            if has_manual_tags(tag_row):
                continue
            metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            has_people_count = bool(tag_row and tag_row.get("people_count") is not None)
            if not force_recount and has_people_count and not _has_face_metadata_backfill_needed(metadata):
                skipped_existing_rows += 1
                continue
            image_urls = _pick_autocount_urls(row)
            if not image_urls:
                continue
            trr_show_eligible = _is_trr_show_eligible(
                db,
                metadata=metadata,
                request_show_id=request_show_id,
                request_show_name=request_show_name,
                show_lookup_by_alias=show_lookup_by_alias,
                show_exists_cache=show_exists_cache,
                show_name_cache=show_name_cache,
            )
            to_process.append(
                {
                    "photo_id": str(row["id"]),
                    "image_urls": image_urls,
                    "row": row,
                    "tag_row": tag_row,
                    "trr_show_eligible": trr_show_eligible,
                }
            )

        total = len(to_process)
        _record_stage_row_stats(
            stage_stats,
            attempted_rows=total,
            skipped_existing_rows=skipped_existing_rows,
        )
        processed_rows = 0
        safe_batch_size = max(1, int(tagging_batch_size))
        for chunk in _chunked(to_process, safe_batch_size):
            prepared_chunk: list[dict[str, Any]] = []
            for item in chunk:
                row = item["row"]
                tag_row = item["tag_row"]
                existing_people_names = tag_row.get("people_names") if tag_row else None
                existing_people_ids = tag_row.get("people_ids") if tag_row else None
                allow_identity_assignment = bool(force_recount) or bool(item.get("trr_show_eligible"))
                candidate_person_ids = _build_identity_candidate_person_ids(
                    db=db,
                    allow_identity_assignment=allow_identity_assignment,
                    owner_person_id=person_id,
                    tagged_people_ids=existing_people_ids,
                    tagged_people_names=existing_people_names,
                    metadata_signals=[
                        existing_people_names,
                        row.get("people_names"),
                        row.get("title_names"),
                        row.get("caption"),
                        row.get("source_page_url"),
                        row.get("metadata"),
                    ],
                    person_name_id_cache=person_name_id_cache,
                )
                person_reference_images = _resolve_runtime_person_reference_pools(
                    db,
                    candidate_person_ids=candidate_person_ids,
                    request_show_id=request_show_id,
                    request_show_name=request_show_name,
                    reference_cache=reference_pool_cache,
                    person_id_name_cache=person_id_name_cache,
                )
                prepared_chunk.append(
                    {
                        **item,
                        "existing_people_names": existing_people_names,
                        "existing_people_ids": existing_people_ids,
                        "allow_identity_assignment": allow_identity_assignment,
                        "candidate_person_ids": candidate_person_ids,
                        "person_reference_images": person_reference_images,
                    }
                )

            batch_requests: list[dict[str, object]] = []
            for item in prepared_chunk:
                image_urls = item.get("image_urls") if isinstance(item.get("image_urls"), list) else []
                first_url = str(image_urls[0]).strip() if image_urls else ""
                if not first_url:
                    batch_requests.append({})
                    continue
                batch_requests.append(
                    {
                        "image_url": first_url,
                        "candidate_person_ids": item.get("candidate_person_ids"),
                        "owner_person_id": person_id,
                        "owner_reference_images": owner_reference_images,
                        "person_reference_images": item.get("person_reference_images"),
                        "prefer_fast_pass": bool(prefer_fast_pass),
                    }
                )

            batch_results: list[Any] = [None for _ in prepared_chunk]
            if batch_requests:
                try:
                    batch_results = count_people_batch(
                        batch_requests,
                        prefer_fast_pass=bool(prefer_fast_pass),
                    )
                except ScreenalyticsUnavailableError as exc:
                    logger.warning("Auto-count halted for %s: %s", person_id, exc)
                    auto_counts_failed += len(prepared_chunk)
                    if failed_photo_ids is not None:
                        failed_photo_ids.extend([str(item.get("photo_id") or "") for item in prepared_chunk])
                    break
                except ScreenalyticsClientError as exc:
                    logger.warning("Batch auto-count failed for %s, falling back to single calls: %s", person_id, exc)
                    batch_results = [None for _ in prepared_chunk]

            for local_idx, item in enumerate(prepared_chunk, start=1):
                processed_rows += 1
                idx = processed_rows
                row = item["row"]
                existing_people_names = item.get("existing_people_names")
                existing_people_ids = item.get("existing_people_ids")
                allow_identity_assignment = bool(item.get("allow_identity_assignment"))
                candidate_person_ids = item.get("candidate_person_ids")
                person_reference_images = item.get("person_reference_images")
                auto_counts_attempted += 1
                result = batch_results[local_idx - 1] if local_idx - 1 < len(batch_results) else None
                image_urls = item.get("image_urls") if isinstance(item.get("image_urls"), list) else []
                selected_image_url: str | None = (
                    str(image_urls[0]).strip() if result is not None and image_urls else None
                )
                last_error: ScreenalyticsClientError | None = None
                service_unavailable_error: ScreenalyticsUnavailableError | None = None

                if result is None:
                    for image_url in image_urls:
                        try:
                            if candidate_person_ids:
                                result = count_people(
                                    image_url,
                                    candidate_person_ids=candidate_person_ids,
                                    owner_person_id=person_id,
                                    owner_reference_images=owner_reference_images,
                                    person_reference_images=person_reference_images or None,
                                    prefer_fast_pass=bool(prefer_fast_pass),
                                )
                            else:
                                result = count_people(
                                    image_url,
                                    owner_person_id=person_id,
                                    owner_reference_images=owner_reference_images,
                                    person_reference_images=person_reference_images or None,
                                    prefer_fast_pass=bool(prefer_fast_pass),
                                )
                            selected_image_url = image_url
                            break
                        except TypeError:
                            if candidate_person_ids:
                                try:
                                    result = count_people(image_url, candidate_person_ids=candidate_person_ids)
                                except TypeError:
                                    result = count_people(image_url)
                            else:
                                result = count_people(image_url)
                            selected_image_url = image_url
                            break
                        except ScreenalyticsUnavailableError as exc:
                            service_unavailable_error = exc
                            last_error = exc
                            break
                        except ScreenalyticsClientError as exc:
                            last_error = exc

                if service_unavailable_error is not None:
                    auto_counts_failed += 1
                    if failed_photo_ids is not None:
                        failed_photo_ids.append(item["photo_id"])
                    logger.warning("Auto-count halted for %s: %s", person_id, service_unavailable_error)
                    break

                try:
                    if result is None:
                        diagnostics_local["auto_detect_failed_rows"] += 1
                        if failed_photo_ids is not None:
                            failed_photo_ids.append(item["photo_id"])
                        raise last_error or ScreenalyticsClientError("Unable to auto-count image")
                    diagnostics_local["auto_detect_success_rows"] += 1
                    if not allow_identity_assignment:
                        diagnostics_local["auto_identity_skipped_non_trr_show"] += 1
                    ref_profile = getattr(result, "reference_profile", None)
                    if owner_reference_sync_cb and isinstance(ref_profile, dict):
                        used_refs = ref_profile.get("used")
                        if isinstance(used_refs, list):
                            owner_reference_sync_cb([entry for entry in used_refs if isinstance(entry, dict)])
                            owner_reference_sync_cb = None
                    face_boxes, row_diagnostics = _build_detection_boxes(
                        result,
                        tagged_people_ids=existing_people_ids,
                        tagged_people_names=existing_people_names,
                        owner_person_id=person_id,
                        owner_person_name=owner_person_name,
                        allow_identity_assignment=allow_identity_assignment,
                    )
                    _merge_counter_fields(diagnostics_local, row_diagnostics, AUTO_COUNT_DIAGNOSTIC_FIELDS)
                    auto_people_ids, auto_people_names = _auto_people_from_face_boxes(face_boxes)
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
                        if _can_reuse_face_crop_cache(
                            previous_face_boxes=existing_meta.get("face_boxes"),
                            previous_face_crops=existing_meta.get("face_crops"),
                            next_face_boxes=face_boxes,
                        ):
                            face_crops = cast(list[dict[str, Any]], existing_meta.get("face_crops"))
                            diagnostics_local["auto_crop_cache_success_rows"] += 1
                        else:
                            face_crops = generate_and_upload_face_crops(
                                entity_kind="cast_photo",
                                entity_id=item["photo_id"],
                                image_url=selected_image_url,
                                face_boxes=cast(list[Mapping[str, Any]], face_boxes),
                                size=256,
                            )
                            if face_crops:
                                diagnostics_local["auto_crop_cache_success_rows"] += 1
                            else:
                                diagnostics_local["auto_crop_cache_failed_rows"] += 1
                    else:
                        diagnostics_local["auto_crop_cache_success_rows"] += 1
                    face_crop_counts = _count_face_crop_sources(face_boxes, face_crops)
                    diagnostics_local["auto_face_crops_generated"] += face_crop_counts[0]
                    diagnostics_local["auto_person_fallback_crops_generated"] += face_crop_counts[1]
                    if existing_meta.get("face_boxes") != face_boxes:
                        existing_meta["face_boxes"] = face_boxes
                        metadata_changed = True
                    if existing_meta.get("face_crops") != face_crops:
                        existing_meta["face_crops"] = face_crops
                        metadata_changed = True
                    crop_payload = _owner_face_crop_payload(
                        face_boxes,
                        owner_person_id=person_id,
                        owner_person_name=owner_person_name,
                    )
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
                            diagnostics_local["auto_persist_failed_rows"] += 1
                            auto_counts_failed += 1
                            if failed_photo_ids is not None:
                                failed_photo_ids.append(item["photo_id"])
                            if progress_cb:
                                progress_cb(idx, total)
                            continue
                    diagnostics_local["auto_persist_success_rows"] += 1
                    auto_counts_succeeded += 1
                except ScreenalyticsClientError as exc:
                    auto_counts_failed += 1
                    if result is not None:
                        diagnostics_local["auto_persist_failed_rows"] += 1
                    if failed_photo_ids is not None:
                        failed_photo_ids.append(item["photo_id"])
                    logger.warning("Auto-count failed for %s: %s", row.get("id"), exc)
                if progress_cb:
                    progress_cb(idx, total)
    except Exception as exc:
        logger.exception("Auto-count setup failed for %s: %s", person_id, exc)

    if diagnostics is not None:
        _merge_counter_fields(diagnostics, diagnostics_local, AUTO_COUNT_DIAGNOSTIC_FIELDS)
    return auto_counts_attempted, auto_counts_succeeded, auto_counts_failed


def _auto_count_media_links(
    db: SupabaseAdminClient,
    person_id: str,
    *,
    owner_person_name: str | None = None,
    owner_reference_images: list[dict[str, Any]] | None = None,
    owner_reference_sync_cb: Callable[[list[dict[str, Any]]], None] | None = None,
    force_recount: bool = False,
    media_link_ids: list[str] | None = None,
    request_show_id: UUID | None = None,
    request_show_name: str | None = None,
    progress_cb: Callable[[int, int], None] | None = None,
    diagnostics: dict[str, int] | None = None,
    stage_stats: dict[str, int] | None = None,
    failed_link_ids: list[str] | None = None,
    tagging_batch_size: int = 32,
    prefer_fast_pass: bool = True,
) -> tuple[int, int, int]:
    attempted = 0
    succeeded = 0
    failed = 0
    diagnostics_local = _empty_auto_count_diagnostics()

    try:
        from trr_backend.clients.screenalytics import (
            ScreenalyticsClientError,
            ScreenalyticsUnavailableError,
            count_people,
            count_people_batch,
            get_screenalytics_unavailable_state,
            is_screenalytics_configured,
        )
        from trr_backend.repositories.media_links import (
            has_manual_people_tags,
            has_people_count,
        )

        if not is_screenalytics_configured():
            if diagnostics is not None:
                _merge_counter_fields(diagnostics, diagnostics_local, AUTO_COUNT_DIAGNOSTIC_FIELDS)
            return attempted, succeeded, failed
        unavailable, _, _ = get_screenalytics_unavailable_state()
        if unavailable:
            if diagnostics is not None:
                _merge_counter_fields(diagnostics, diagnostics_local, AUTO_COUNT_DIAGNOSTIC_FIELDS)
            return attempted, succeeded, failed

        normalized_media_link_ids = _normalize_scope_ids(media_link_ids)
        if media_link_ids is not None and not normalized_media_link_ids:
            _record_stage_row_stats(stage_stats, attempted_rows=0, skipped_existing_rows=0)
            if diagnostics is not None:
                _merge_counter_fields(diagnostics, diagnostics_local, AUTO_COUNT_DIAGNOSTIC_FIELDS)
            return attempted, succeeded, failed
        try:
            rows = _fetch_person_media_link_rows(
                db,
                person_id,
                link_ids=normalized_media_link_ids,
            )
        except TypeError:
            rows = _fetch_person_media_link_rows(db, person_id)
            if normalized_media_link_ids:
                allowed_fallback_ids = set(normalized_media_link_ids)
                rows = [row for row in rows if str(row.get("id") or "").strip() in allowed_fallback_ids]
        allowed_link_ids = set(normalized_media_link_ids or [])
        show_lookup_by_alias: dict[str, dict[str, Any]] | None = None
        try:
            _, show_lookup_by_alias, _ = _build_show_lookup_maps(db)
        except Exception:  # noqa: BLE001
            show_lookup_by_alias = {}
        show_exists_cache: dict[str, bool] = {}
        show_name_cache: dict[str, str | None] = {}
        person_name_id_cache: dict[str, str | None] = {}
        person_id_name_cache: dict[str, str | None] = {}
        reference_pool_cache: dict[str, list[dict[str, Any]]] = {}
        to_process: list[dict[str, Any]] = []
        skipped_existing_rows = 0
        for row in rows:
            row_id = str(row.get("id") or "").strip()
            if allowed_link_ids and row_id not in allowed_link_ids:
                continue
            context = row.get("context") if isinstance(row.get("context"), dict) else {}
            if has_manual_people_tags(context):
                continue
            if not force_recount and has_people_count(context) and not _has_face_metadata_backfill_needed(context):
                skipped_existing_rows += 1
                continue
            urls = _build_media_link_autocount_urls(row)
            if not urls:
                continue
            trr_show_eligible = _is_trr_show_eligible(
                db,
                metadata=context,
                request_show_id=request_show_id,
                request_show_name=request_show_name,
                show_lookup_by_alias=show_lookup_by_alias,
                show_exists_cache=show_exists_cache,
                show_name_cache=show_name_cache,
            )
            to_process.append(
                {
                    "row": row,
                    "urls": urls,
                    "context": dict(context or {}),
                    "trr_show_eligible": trr_show_eligible,
                }
            )

        total = len(to_process)
        _record_stage_row_stats(
            stage_stats,
            attempted_rows=total,
            skipped_existing_rows=skipped_existing_rows,
        )
        processed_rows = 0
        safe_batch_size = max(1, int(tagging_batch_size))
        for chunk in _chunked(to_process, safe_batch_size):
            prepared_chunk: list[dict[str, Any]] = []
            for item in chunk:
                row = item["row"]
                context = item["context"]
                allow_identity_assignment = bool(force_recount) or bool(item.get("trr_show_eligible"))
                candidate_person_ids = _build_identity_candidate_person_ids(
                    db=db,
                    allow_identity_assignment=allow_identity_assignment,
                    owner_person_id=person_id,
                    tagged_people_ids=context.get("people_ids"),
                    tagged_people_names=context.get("people_names"),
                    metadata_signals=[
                        context.get("people_names"),
                        context.get("titles"),
                        context.get("caption"),
                        context.get("name"),
                        context.get("title"),
                        context.get("episode"),
                        context.get("original_source_page"),
                        context,
                        row.get("caption"),
                        row.get("metadata"),
                    ],
                    person_name_id_cache=person_name_id_cache,
                )
                person_reference_images = _resolve_runtime_person_reference_pools(
                    db,
                    candidate_person_ids=candidate_person_ids,
                    request_show_id=request_show_id,
                    request_show_name=request_show_name,
                    reference_cache=reference_pool_cache,
                    person_id_name_cache=person_id_name_cache,
                )
                prepared_chunk.append(
                    {
                        **item,
                        "allow_identity_assignment": allow_identity_assignment,
                        "candidate_person_ids": candidate_person_ids,
                        "person_reference_images": person_reference_images,
                    }
                )

            batch_requests: list[dict[str, object]] = []
            for item in prepared_chunk:
                urls = item.get("urls") if isinstance(item.get("urls"), list) else []
                first_url = str(urls[0]).strip() if urls else ""
                if not first_url:
                    batch_requests.append({})
                    continue
                batch_requests.append(
                    {
                        "image_url": first_url,
                        "candidate_person_ids": item.get("candidate_person_ids"),
                        "owner_person_id": person_id,
                        "owner_reference_images": owner_reference_images,
                        "person_reference_images": item.get("person_reference_images"),
                        "prefer_fast_pass": bool(prefer_fast_pass),
                    }
                )

            batch_results: list[Any] = [None for _ in prepared_chunk]
            if batch_requests:
                try:
                    batch_results = count_people_batch(
                        batch_requests,
                        prefer_fast_pass=bool(prefer_fast_pass),
                    )
                except ScreenalyticsUnavailableError as exc:
                    logger.warning("Auto-count media_links halted for %s: %s", person_id, exc)
                    failed += len(prepared_chunk)
                    if failed_link_ids is not None:
                        failed_link_ids.extend([str(item.get("row", {}).get("id") or "") for item in prepared_chunk])
                    break
                except ScreenalyticsClientError as exc:
                    logger.warning(
                        "Batch auto-count media_links failed for %s, falling back to single calls: %s",
                        person_id,
                        exc,
                    )
                    batch_results = [None for _ in prepared_chunk]

            for local_idx, item in enumerate(prepared_chunk, start=1):
                processed_rows += 1
                idx = processed_rows
                attempted += 1
                row = item["row"]
                context = item["context"]
                allow_identity_assignment = bool(item.get("allow_identity_assignment"))
                candidate_person_ids = item.get("candidate_person_ids")
                person_reference_images = item.get("person_reference_images")
                result = batch_results[local_idx - 1] if local_idx - 1 < len(batch_results) else None
                urls = item.get("urls") if isinstance(item.get("urls"), list) else []
                selected_image_url: str | None = str(urls[0]).strip() if result is not None and urls else None
                last_error: ScreenalyticsClientError | None = None
                service_unavailable_error: ScreenalyticsUnavailableError | None = None

                if result is None:
                    for image_url in urls:
                        try:
                            if candidate_person_ids:
                                result = count_people(
                                    image_url,
                                    candidate_person_ids=candidate_person_ids,
                                    owner_person_id=person_id,
                                    owner_reference_images=owner_reference_images,
                                    person_reference_images=person_reference_images or None,
                                    prefer_fast_pass=bool(prefer_fast_pass),
                                )
                            else:
                                result = count_people(
                                    image_url,
                                    owner_person_id=person_id,
                                    owner_reference_images=owner_reference_images,
                                    person_reference_images=person_reference_images or None,
                                    prefer_fast_pass=bool(prefer_fast_pass),
                                )
                            selected_image_url = image_url
                            break
                        except TypeError:
                            if candidate_person_ids:
                                try:
                                    result = count_people(image_url, candidate_person_ids=candidate_person_ids)
                                except TypeError:
                                    result = count_people(image_url)
                            else:
                                result = count_people(image_url)
                            selected_image_url = image_url
                            break
                        except ScreenalyticsUnavailableError as exc:
                            service_unavailable_error = exc
                            last_error = exc
                            break
                        except ScreenalyticsClientError as exc:
                            last_error = exc

                if service_unavailable_error is not None:
                    failed += 1
                    if failed_link_ids is not None:
                        failed_link_ids.append(str(row.get("id") or ""))
                    logger.warning("Auto-count media_links halted for %s: %s", person_id, service_unavailable_error)
                    break
                try:
                    if result is None:
                        diagnostics_local["auto_detect_failed_rows"] += 1
                        if failed_link_ids is not None:
                            failed_link_ids.append(str(row.get("id") or ""))
                        raise last_error or ScreenalyticsClientError("Unable to auto-count image")
                    diagnostics_local["auto_detect_success_rows"] += 1
                    if not allow_identity_assignment:
                        diagnostics_local["auto_identity_skipped_non_trr_show"] += 1
                    ref_profile = getattr(result, "reference_profile", None)
                    if owner_reference_sync_cb and isinstance(ref_profile, dict):
                        used_refs = ref_profile.get("used")
                        if isinstance(used_refs, list):
                            owner_reference_sync_cb([entry for entry in used_refs if isinstance(entry, dict)])
                            owner_reference_sync_cb = None
                    face_boxes, row_diagnostics = _build_detection_boxes(
                        result,
                        tagged_people_ids=context.get("people_ids"),
                        tagged_people_names=context.get("people_names"),
                        owner_person_id=person_id,
                        owner_person_name=owner_person_name,
                        allow_identity_assignment=allow_identity_assignment,
                    )
                    _merge_counter_fields(diagnostics_local, row_diagnostics, AUTO_COUNT_DIAGNOSTIC_FIELDS)
                    auto_people_ids, auto_people_names = _auto_people_from_face_boxes(face_boxes)
                    context["people_count"] = result.people_count
                    context["people_count_source"] = "auto"
                    context["people_count_detector"] = result.detector
                    previous_face_boxes = context.get("face_boxes")
                    previous_face_crops = context.get("face_crops")
                    context["face_boxes"] = face_boxes
                    face_crops: list[dict[str, Any]] = []
                    if selected_image_url and face_boxes:
                        if _can_reuse_face_crop_cache(
                            previous_face_boxes=previous_face_boxes,
                            previous_face_crops=previous_face_crops,
                            next_face_boxes=face_boxes,
                        ):
                            face_crops = cast(list[dict[str, Any]], previous_face_crops)
                            diagnostics_local["auto_crop_cache_success_rows"] += 1
                        else:
                            media_asset_id = str(row.get("media_asset_id") or row.get("id") or "").strip()
                            face_crops = generate_and_upload_face_crops(
                                entity_kind="media_asset",
                                entity_id=media_asset_id,
                                image_url=selected_image_url,
                                face_boxes=cast(list[Mapping[str, Any]], face_boxes),
                                size=256,
                            )
                            if face_crops:
                                diagnostics_local["auto_crop_cache_success_rows"] += 1
                            else:
                                diagnostics_local["auto_crop_cache_failed_rows"] += 1
                    else:
                        diagnostics_local["auto_crop_cache_success_rows"] += 1
                    face_crop_counts = _count_face_crop_sources(face_boxes, face_crops)
                    diagnostics_local["auto_face_crops_generated"] += face_crop_counts[0]
                    diagnostics_local["auto_person_fallback_crops_generated"] += face_crop_counts[1]
                    context["face_crops"] = face_crops
                    if (
                        not (isinstance(context.get("people_ids"), list) and context.get("people_ids"))
                        and auto_people_ids
                    ):
                        context["people_ids"] = auto_people_ids
                    if (
                        not (isinstance(context.get("people_names"), list) and context.get("people_names"))
                        and auto_people_names
                    ):
                        context["people_names"] = auto_people_names
                    crop_payload = _owner_face_crop_payload(
                        face_boxes,
                        owner_person_id=person_id,
                        owner_person_name=owner_person_name,
                    )
                    if crop_payload is not None and not _is_manual_thumbnail_crop(context.get("thumbnail_crop")):
                        context["thumbnail_crop"] = crop_payload
                    db.schema("core").table("media_links").update(
                        {"context": context, "updated_at": datetime.now(UTC).isoformat()}
                    ).eq("id", row["id"]).execute()
                    diagnostics_local["auto_persist_success_rows"] += 1
                    succeeded += 1
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    if result is not None:
                        diagnostics_local["auto_persist_failed_rows"] += 1
                    if failed_link_ids is not None:
                        failed_link_ids.append(str(row.get("id") or ""))
                    logger.warning("Auto-count media_link failed for %s: %s", row.get("id"), exc)
                if progress_cb:
                    progress_cb(idx, total)
    except Exception as exc:
        logger.exception("Auto-count media_links setup failed for %s: %s", person_id, exc)

    if diagnostics is not None:
        _merge_counter_fields(diagnostics, diagnostics_local, AUTO_COUNT_DIAGNOSTIC_FIELDS)
    return attempted, succeeded, failed


def _resize_person_gallery_images(
    db: SupabaseAdminClient,
    person_id: str,
    sources: list[SourceType],
    *,
    photo_ids: list[str] | None = None,
    media_link_ids: list[str] | None = None,
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
    normalized_photo_ids = _normalize_scope_ids(photo_ids)
    normalized_media_link_ids = _normalize_scope_ids(media_link_ids)

    try:
        from api.routers.admin_image_counts import auto_count_cast_photo, auto_count_media_asset
        from trr_backend.media.image_variants import (
            generate_cast_photo_variants,
            generate_media_asset_variants,
        )

        cast_rows: list[dict[str, Any]] = []
        if not (photo_ids is not None and not normalized_photo_ids):
            cast_query = (
                db.schema("core")
                .table("cast_photos")
                .select("id, source, hosted_url, metadata")
                .eq("person_id", person_id)
                .in_("source", candidate_sources)
                .not_.is_("hosted_url", "null")
            )
            if normalized_photo_ids:
                cast_query = cast_query.in_("id", normalized_photo_ids)
            cast_rows = cast_query.execute().data or []
        media_rows: list[dict[str, Any]] = []
        if not (media_link_ids is not None and not normalized_media_link_ids):
            media_rows = _fetch_person_media_link_rows(
                db,
                person_id,
                link_ids=normalized_media_link_ids,
            )

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
        is_episode_like = bool(
            title_type.upper() == "TVEPISODE" or season_number is not None or episode_number is not None or show_name
        )
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
            "is_episode_like": is_episode_like,
        }
    return out


_WWHL_SHOW_NAME = "Watch What Happens Live with Andy Cohen"
_WWHL_SHOW_IMDB_ID = "tt2057880"


def _is_wwhl_show_name(value: str | None) -> bool:
    normalized = str(value or "").strip().lower()
    return bool(normalized) and ("watch what happens live" in normalized or "wwhl" in normalized)


def _load_person_wwhl_episode_imdb_ids_from_credits(
    db: SupabaseAdminClient,
    person_id: str,
) -> set[str]:
    normalized_person_id = str(person_id or "").strip()
    if not normalized_person_id:
        return set()

    try:
        response = (
            db.schema("core")
            .table("v_episode_appearances_from_credits")
            .select("show_id,imdb_episode_title_ids")
            .eq("person_id", normalized_person_id)
            .limit(5000)
            .execute()
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("WWHL credit lookup failed person_id=%s error=%s", normalized_person_id, exc)
        return set()
    if hasattr(response, "error") and response.error:
        logger.debug("WWHL credit lookup error person_id=%s error=%s", normalized_person_id, response.error)
        return set()

    rows = response.data if isinstance(getattr(response, "data", None), list) else []
    if not rows:
        return set()

    show_ids = {
        str(row.get("show_id") or "").strip()
        for row in rows
        if isinstance(row, dict) and str(row.get("show_id") or "").strip()
    }
    if not show_ids:
        return set()

    try:
        shows_response = db.schema("core").table("shows").select("id,name").in_("id", list(show_ids)).execute()
    except Exception as exc:  # noqa: BLE001
        logger.debug("WWHL show lookup failed person_id=%s error=%s", normalized_person_id, exc)
        return set()
    if hasattr(shows_response, "error") and shows_response.error:
        logger.debug("WWHL show lookup error person_id=%s error=%s", normalized_person_id, shows_response.error)
        return set()

    wwhl_show_ids = {
        str(show_row.get("id") or "").strip()
        for show_row in (shows_response.data or [])
        if isinstance(show_row, dict) and _is_wwhl_show_name(show_row.get("name"))
    }
    if not wwhl_show_ids:
        return set()

    out: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        show_id = str(row.get("show_id") or "").strip()
        if show_id not in wwhl_show_ids:
            continue
        imdb_ids = row.get("imdb_episode_title_ids")
        if not isinstance(imdb_ids, list):
            continue
        for imdb_id in imdb_ids:
            normalized = str(imdb_id or "").strip().lower()
            if normalized:
                out.add(normalized)
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
        external_ids_response is not None and hasattr(external_ids_response, "error") and external_ids_response.error
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
    *,
    person_wwhl_episode_imdb_ids: set[str] | None = None,
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
    wwhl_credit_episode_ids = {
        str(imdb_id or "").strip().lower()
        for imdb_id in (person_wwhl_episode_imdb_ids or set())
        if str(imdb_id or "").strip()
    }
    show_lookup_by_imdb_id, show_lookup_by_alias, show_lookup_by_id = _build_show_lookup_maps(db)

    if not episodes_by_imdb and not imdb_fallback_by_id and not wwhl_credit_episode_ids:
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
            imdb_id_norm = str(imdb_id or "").strip().lower()
            if imdb_id_norm and imdb_id_norm in wwhl_credit_episode_ids:
                title_names = row.get("title_names")
                episode_title: str | None = None
                if isinstance(title_names, list):
                    for candidate_title in title_names:
                        if isinstance(candidate_title, str) and candidate_title.strip():
                            episode_title = candidate_title.strip()
                            break
                row_metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
                fallback = {
                    "episode_imdb_id": str(imdb_id).strip(),
                    "episode_title": episode_title,
                    "season_number": _to_int(row_metadata.get("season_number")),
                    "episode_number": _to_int(row_metadata.get("episode_number")),
                    "episode_air_date": row_metadata.get("episode_air_date"),
                    "show_name": _WWHL_SHOW_NAME,
                    "show_imdb_id": _WWHL_SHOW_IMDB_ID,
                    "show_short_code": None,
                    "imdb_title_type": (
                        str(row_metadata.get("imdb_title_type")).strip()
                        if str(row_metadata.get("imdb_title_type") or "").strip()
                        else "TVEpisode"
                    ),
                }
                break
        if not episode and not fallback:
            metadata = dict(row.get("metadata") or {})
            show_context_source = str(metadata.get("show_context_source") or "").strip().lower()
            has_show_context = bool(
                str(metadata.get("show_id") or "").strip()
                or str(metadata.get("show_name") or "").strip()
                or str(metadata.get("show_imdb_id") or "").strip()
            )
            has_structured_episode_identity = bool(
                str(metadata.get("imdb_title_type") or "").strip().upper() == "TVEPISODE"
                or str(metadata.get("episode_imdb_id") or "").strip()
                or str(metadata.get("episode_title") or "").strip()
                or _to_int(metadata.get("season_number")) is not None
                or _to_int(metadata.get("episode_number")) is not None
            )
            has_episode_show_fallback = bool(
                str(metadata.get("imdb_fallback_show_name") or "").strip()
                or str(metadata.get("imdb_fallback_show_imdb_id") or "").strip()
            )
            if (
                has_show_context
                and has_structured_episode_identity
                and not has_episode_show_fallback
                and show_context_source in {"", "request_context", "request_context_inferred", "show_context_request"}
            ):
                metadata["show_id"] = None
                metadata["show_name"] = None
                metadata["show_imdb_id"] = None
                metadata["show_short_code"] = None
                metadata["show_context_source"] = "request_context_rejected"
                metadata["show_context_repair_reason"] = "missing_corroboration"
                metadata["episode_id"] = None
                metadata["episode_imdb_id"] = None
                metadata["episode_title"] = None
                metadata["episode_number"] = None
                metadata["season_number"] = None
                metadata["episode_air_date"] = None
                metadata["source_created_at"] = None
                row["metadata"] = metadata
                if row.get("season") is not None:
                    row["season"] = None
                tagged += 1
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
            fallback_title_type = str(fallback.get("imdb_title_type") or "").strip()
            fallback_title_type_upper = fallback_title_type.upper()
            fallback_is_episode_like = bool(fallback.get("is_episode_like")) or bool(
                fallback_title_type_upper in {"TVEPISODE", "EPISODE"}
                or str(fallback.get("episode_imdb_id") or "").strip()
                or _to_int(fallback.get("season_number")) is not None
                or _to_int(fallback.get("episode_number")) is not None
            )
            fallback_title_id = str(fallback.get("episode_imdb_id") or "").strip() or None
            if fallback_title_id:
                metadata["imdb_title_id"] = fallback_title_id.lower()
                metadata["imdb_title_url"] = f"https://www.imdb.com/title/{fallback_title_id.lower()}/"
            if fallback_title_type:
                metadata["imdb_title_type"] = fallback_title_type
                credit_media_type = _format_imdb_credit_media_type(fallback_title_type)
                if credit_media_type:
                    metadata["imdb_credit_media_type"] = credit_media_type

            if not fallback_is_episode_like:
                metadata["episode_id"] = None
                metadata["episode_imdb_id"] = None
                metadata["episode_title"] = None
                metadata["episode_number"] = None
                metadata["season_number"] = None
                metadata["episode_air_date"] = None
                metadata["imdb_fallback_show_name"] = None
                metadata["imdb_fallback_show_imdb_id"] = None
                metadata["source_created_at"] = None
                metadata["show_id"] = None
                metadata["show_name"] = None
                metadata["show_imdb_id"] = None
                metadata["show_short_code"] = None
                metadata["show_context_source"] = "request_context_rejected"
                metadata["show_context_repair_reason"] = "non_episode_title_id"
                row["metadata"] = metadata
                if row.get("season") is not None:
                    row["season"] = None
                tagged += 1
                continue

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
    show_context_source = str(metadata.get("show_context_source") or "").strip().lower()
    if show_context_source in {"episode_table", "imdb_episode_unresolved", "imdb_title_fallback"}:
        return True
    if isinstance(metadata.get("episode_imdb_id"), str) and str(metadata.get("episode_imdb_id")).strip():
        return True
    if isinstance(metadata.get("episode_title"), str) and str(metadata.get("episode_title")).strip():
        return True
    if _to_int(metadata.get("season_number")) is not None:
        return True
    if _to_int(metadata.get("episode_number")) is not None:
        return True
    imdb_title_type = str(metadata.get("imdb_title_type") or "").strip().upper()
    if imdb_title_type == "TVEPISODE":
        return True
    context_type = str(row.get("context_type") or "").strip().lower()
    if "episode" in context_type:
        return True
    image_type = str(metadata.get("imdb_image_type") or "").strip().lower()
    if image_type in {"still_frame", "still frame", "episode_still", "episode still"}:
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
        has_show_metadata = bool(before_show_id and isinstance(before_show_id, str) and before_show_id.strip()) or bool(
            before_show_name and isinstance(before_show_name, str) and before_show_name.strip()
        )
        episode_evidence = _is_imdb_episode_or_title_evidence(row, metadata)
        show_mismatch = bool(
            has_show_metadata
            and (
                (
                    show_id_str
                    and isinstance(before_show_id, str)
                    and before_show_id.strip()
                    and before_show_id != show_id_str
                )
                or (
                    requested_show_imdb_id
                    and isinstance(metadata.get("show_imdb_id"), str)
                    and str(metadata.get("show_imdb_id")).strip()
                    and str(metadata.get("show_imdb_id")).strip() != requested_show_imdb_id
                )
            )
        )
        has_unresolved_imdb_episode_evidence = show_context_source == "imdb_episode_unresolved" or (
            not has_show_metadata and episode_evidence
        )

        should_attempt_episode_inference = bool(
            has_unresolved_imdb_episode_evidence
            or (
                episode_evidence
                and (
                    not has_show_metadata
                    or show_mismatch
                    or show_context_source in {"request_context", "request_context_inferred", "show_context_request"}
                )
            )
        )
        if should_attempt_episode_inference:
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

        imdb_title_type = str(metadata.get("imdb_title_type") or "").strip().upper()
        is_non_episode_title = bool(imdb_title_type and imdb_title_type != "TVEPISODE")
        is_non_episode_rejected = (
            show_context_source == "request_context_rejected"
            and str(metadata.get("show_context_repair_reason") or "").strip().lower() == "non_episode_title_id"
        )
        if not has_show_metadata and (is_non_episode_title or is_non_episode_rejected):
            row["metadata"] = metadata
            continue

        if not has_show_metadata:
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


def _normalize_imdb_title_id(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if re.fullmatch(r"tt\d+", raw, flags=re.IGNORECASE):
        return raw.lower()
    match = IMDB_TITLE_ID_RE.search(raw)
    if match:
        return match.group(1).lower()
    return None


def _normalize_imdb_title_url(value: Any) -> str | None:
    if isinstance(value, str) and value.strip().lower().startswith(("http://", "https://")):
        title_id = _normalize_imdb_title_id(value)
        if title_id:
            return f"https://www.imdb.com/title/{title_id}/"
    title_id = _normalize_imdb_title_id(value if isinstance(value, str) else None)
    if not title_id:
        return None
    return f"https://www.imdb.com/title/{title_id}/"


def _resolve_imdb_title_identity(row: dict[str, Any], metadata: dict[str, Any]) -> tuple[str | None, str | None]:
    title_id = _normalize_imdb_title_id(metadata.get("imdb_title_id"))
    if not title_id:
        title_id = _normalize_imdb_title_id(metadata.get("imdb_title_url"))
    if not title_id:
        title_ids = row.get("title_imdb_ids")
        if isinstance(title_ids, list):
            for candidate in title_ids:
                normalized = _normalize_imdb_title_id(candidate)
                if normalized:
                    title_id = normalized
                    break
    title_url = _normalize_imdb_title_url(title_id)
    if not title_url:
        title_url = _normalize_imdb_title_url(metadata.get("imdb_title_url"))
    if title_id is None and title_url:
        title_id = _normalize_imdb_title_id(title_url)
    return title_id, title_url


def _format_imdb_credit_media_type(value: Any) -> str | None:
    normalized = str(value or "").strip().upper().replace(" ", "").replace("-", "")
    if not normalized:
        return None
    return IMDB_CREDIT_MEDIA_TYPE_BY_TITLE_TYPE.get(normalized)


def _has_imdb_episode_evidence_metadata(row: dict[str, Any], metadata: dict[str, Any]) -> bool:
    imdb_title_type = str(metadata.get("imdb_title_type") or "").strip().upper()
    context_type = str(row.get("context_type") or "").strip().lower()
    image_type = str(metadata.get("imdb_image_type") or "").strip().lower()
    return bool(
        imdb_title_type == "TVEPISODE"
        or (isinstance(metadata.get("episode_imdb_id"), str) and str(metadata.get("episode_imdb_id")).strip())
        or (isinstance(metadata.get("episode_title"), str) and str(metadata.get("episode_title")).strip())
        or metadata.get("season_number") is not None
        or metadata.get("episode_number") is not None
        or ("episode" in context_type)
        or image_type in {"still_frame", "still frame", "episode_still", "episode still"}
    )


def _needs_imdb_metadata_refresh(row: dict[str, Any]) -> bool:
    return _needs_imdb_metadata_refresh_with_show_lookup(
        row,
        show_lookup_by_imdb_id=None,
        show_lookup_by_alias=None,
        show_lookup_by_id=None,
    )


def _resolve_show_row_from_metadata(
    metadata: dict[str, Any],
    *,
    show_lookup_by_imdb_id: dict[str, dict[str, Any]] | None,
    show_lookup_by_alias: dict[str, dict[str, Any]] | None,
    show_lookup_by_id: dict[str, dict[str, Any]] | None,
) -> dict[str, Any] | None:
    show_id = str(metadata.get("show_id") or "").strip()
    if show_id and isinstance(show_lookup_by_id, dict):
        show_row = show_lookup_by_id.get(show_id)
        if isinstance(show_row, dict):
            return show_row

    show_imdb_id = str(metadata.get("show_imdb_id") or "").strip()
    if show_imdb_id and isinstance(show_lookup_by_imdb_id, dict):
        show_row = show_lookup_by_imdb_id.get(show_imdb_id)
        if isinstance(show_row, dict):
            return show_row

    show_name = str(metadata.get("show_name") or "").strip()
    if show_name and isinstance(show_lookup_by_alias, dict):
        show_row = _find_show_row_by_alias(show_lookup_by_alias, show_name)
        if isinstance(show_row, dict):
            return show_row
    return None


def _resolve_fallback_show_row_from_metadata(
    metadata: dict[str, Any],
    *,
    show_lookup_by_imdb_id: dict[str, dict[str, Any]] | None,
    show_lookup_by_alias: dict[str, dict[str, Any]] | None,
) -> dict[str, Any] | None:
    fallback_show_imdb_id = str(metadata.get("imdb_fallback_show_imdb_id") or "").strip()
    if fallback_show_imdb_id and isinstance(show_lookup_by_imdb_id, dict):
        show_row = show_lookup_by_imdb_id.get(fallback_show_imdb_id)
        if isinstance(show_row, dict):
            return show_row

    fallback_show_name = str(metadata.get("imdb_fallback_show_name") or "").strip()
    if fallback_show_name and isinstance(show_lookup_by_alias, dict):
        show_row = _find_show_row_by_alias(show_lookup_by_alias, fallback_show_name)
        if isinstance(show_row, dict):
            return show_row
    return None


def _evaluate_imdb_request_context_staleness(
    row: dict[str, Any],
    *,
    show_lookup_by_imdb_id: dict[str, dict[str, Any]] | None,
    show_lookup_by_alias: dict[str, dict[str, Any]] | None,
    show_lookup_by_id: dict[str, dict[str, Any]] | None,
) -> tuple[bool, str | None]:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    show_context_source = str(metadata.get("show_context_source") or "").strip().lower()
    if show_context_source not in {"", "request_context", "request_context_inferred", "show_context_request"}:
        return False, None

    has_episode_evidence = _has_imdb_episode_evidence_metadata(row, metadata)
    fallback_show_name = str(metadata.get("imdb_fallback_show_name") or "").strip()
    fallback_show_imdb_id = str(metadata.get("imdb_fallback_show_imdb_id") or "").strip()
    has_fallback = bool(fallback_show_name or fallback_show_imdb_id)

    if show_context_source == "request_context_inferred" and not has_episode_evidence:
        return True, "missing_corroboration"

    current_show_row = _resolve_show_row_from_metadata(
        metadata,
        show_lookup_by_imdb_id=show_lookup_by_imdb_id,
        show_lookup_by_alias=show_lookup_by_alias,
        show_lookup_by_id=show_lookup_by_id,
    )
    fallback_show_row = _resolve_fallback_show_row_from_metadata(
        metadata,
        show_lookup_by_imdb_id=show_lookup_by_imdb_id,
        show_lookup_by_alias=show_lookup_by_alias,
    )
    current_show_id = (
        str(current_show_row.get("id") or "").strip() if isinstance(current_show_row, dict) else ""
    ) or None
    fallback_show_id = (
        str(fallback_show_row.get("id") or "").strip() if isinstance(fallback_show_row, dict) else ""
    ) or None
    has_current_show_context = bool(current_show_id or str(metadata.get("show_name") or "").strip())
    imdb_title_type = str(metadata.get("imdb_title_type") or "").strip().upper()
    credit_media_type = str(metadata.get("imdb_credit_media_type") or "").strip().lower()
    has_non_episode_title_type = bool(imdb_title_type and imdb_title_type != "TVEPISODE")
    has_non_episode_credit_type = bool(credit_media_type and credit_media_type not in {"tv episode", "episode"})

    if has_current_show_context and (
        has_non_episode_title_type or (not imdb_title_type and has_non_episode_credit_type)
    ):
        return True, "non_episode_title_id"

    has_explicit_episode_identity = bool(
        str(metadata.get("episode_imdb_id") or "").strip()
        or str(metadata.get("episode_title") or "").strip()
        or _to_int(metadata.get("season_number")) is not None
        or _to_int(metadata.get("episode_number")) is not None
    )
    if has_episode_evidence and has_current_show_context and not has_fallback and not has_explicit_episode_identity:
        return True, "missing_corroboration"

    if has_episode_evidence and not has_fallback and not current_show_id:
        return True, "missing_corroboration"

    if has_fallback and current_show_id and fallback_show_id and current_show_id != fallback_show_id:
        # Episode-level IMDb evidence is authoritative over weak/mismatched request-context tags.
        if has_episode_evidence:
            return False, None
        return True, "stale_request_context_mismatch"

    return False, None


def _needs_imdb_metadata_refresh_with_show_lookup(
    row: dict[str, Any],
    *,
    show_lookup_by_imdb_id: dict[str, dict[str, Any]] | None,
    show_lookup_by_alias: dict[str, dict[str, Any]] | None,
    show_lookup_by_id: dict[str, dict[str, Any]] | None,
) -> bool:
    if str(row.get("source") or "").strip().lower() != "imdb":
        return False
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    title_ids = row.get("title_imdb_ids") if isinstance(row.get("title_imdb_ids"), list) else []
    people_ids = row.get("people_imdb_ids") if isinstance(row.get("people_imdb_ids"), list) else []
    tags = metadata.get("tags") if isinstance(metadata, dict) else None
    has_tags = isinstance(tags, dict) and bool(tags)
    imdb_image_type = metadata.get("imdb_image_type")
    has_image_type = isinstance(imdb_image_type, str) and bool(imdb_image_type.strip())
    show_context_source = str(metadata.get("show_context_source") or "").strip().lower()
    unresolved_show = show_context_source == "imdb_episode_unresolved"

    has_episode_evidence = _has_imdb_episode_evidence_metadata(row, metadata)

    fallback_show_name = str(metadata.get("imdb_fallback_show_name") or "").strip()
    fallback_show_imdb_id = str(metadata.get("imdb_fallback_show_imdb_id") or "").strip()
    has_show_context = bool(
        str(metadata.get("show_id") or "").strip()
        or str(metadata.get("show_name") or "").strip()
        or str(metadata.get("show_imdb_id") or "").strip()
    )
    missing_episode_show_fallback = bool(
        has_episode_evidence
        and not fallback_show_name
        and not fallback_show_imdb_id
        and show_context_source in {"", "request_context", "request_context_inferred", "imdb_title_fallback"}
    )
    title_identity_missing = not bool(_resolve_imdb_title_identity(row, metadata)[0])
    credit_media_type_missing = not bool(
        _format_imdb_credit_media_type(metadata.get("imdb_title_type"))
        or (
            isinstance(metadata.get("imdb_credit_media_type"), str)
            and str(metadata.get("imdb_credit_media_type")).strip()
        )
    )
    needs_episode_repair = bool(
        has_episode_evidence
        and has_show_context
        and show_context_source in {"", "request_context", "request_context_inferred", "show_context_request"}
    )
    stale_request_context, _ = _evaluate_imdb_request_context_staleness(
        row,
        show_lookup_by_imdb_id=show_lookup_by_imdb_id,
        show_lookup_by_alias=show_lookup_by_alias,
        show_lookup_by_id=show_lookup_by_id,
    )

    return (
        (not title_ids)
        or (not people_ids)
        or (not has_tags)
        or (not has_image_type)
        or unresolved_show
        or missing_episode_show_fallback
        or title_identity_missing
        or credit_media_type_missing
        or needs_episode_repair
        or stale_request_context
    )


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
    wwhl_credit_episode_imdb_ids: set[str] | None = None,
    progress_cb: Callable[[int, int, int, int], None] | None = None,
) -> tuple[int, int]:
    from trr_backend.integrations.imdb.person_gallery import (
        fetch_imdb_person_mediaviewer_html,
        parse_imdb_person_mediaviewer_details,
    )
    from trr_backend.repositories.cast_photos import upsert_cast_photos

    rows = _load_existing_imdb_cast_photos_for_person(db, person_id)
    if not rows:
        if progress_cb:
            progress_cb(0, 0, 0, 0)
        return 0, 0

    show_lookup_by_imdb_id, show_lookup_by_alias, show_lookup_by_id = _build_show_lookup_maps(db)
    repair_rows = [
        row
        for row in rows
        if _needs_imdb_metadata_refresh_with_show_lookup(
            row,
            show_lookup_by_imdb_id=show_lookup_by_imdb_id,
            show_lookup_by_alias=show_lookup_by_alias,
            show_lookup_by_id=show_lookup_by_id,
        )
    ]
    if not repair_rows:
        if progress_cb:
            progress_cb(0, 0, 0, 0)
        return 0, 0

    metadata_failures = 0
    reviewed_rows = 0
    changed_rows = 0
    total_rows = len(repair_rows)
    if progress_cb:
        progress_cb(0, total_rows, 0, 0)
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
                details_title_id = _normalize_imdb_title_id(details.get("imdb_title_id"))
                if details_title_id:
                    metadata["imdb_title_id"] = details_title_id
                details_title_url = _normalize_imdb_title_url(details.get("imdb_title_url"))
                if details_title_url:
                    metadata["imdb_title_url"] = details_title_url

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
        reviewed_rows += 1
        changed_rows += 1
        if progress_cb:
            progress_cb(reviewed_rows, total_rows, changed_rows, metadata_failures)

    try:
        effective_wwhl_credit_episode_imdb_ids = (
            wwhl_credit_episode_imdb_ids
            if wwhl_credit_episode_imdb_ids is not None
            else _load_person_wwhl_episode_imdb_ids_from_credits(db, person_id)
        )
        _, episode_failed = _enrich_cast_photos_with_episode_metadata(
            db,
            repair_rows,
            person_wwhl_episode_imdb_ids=effective_wwhl_credit_episode_imdb_ids,
        )
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

    for row in repair_rows:
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        is_stale_request_context, stale_reason = _evaluate_imdb_request_context_staleness(
            row,
            show_lookup_by_imdb_id=show_lookup_by_imdb_id,
            show_lookup_by_alias=show_lookup_by_alias,
            show_lookup_by_id=show_lookup_by_id,
        )
        if not is_stale_request_context:
            continue
        metadata["show_id"] = None
        metadata["show_name"] = None
        metadata["show_imdb_id"] = None
        metadata["show_short_code"] = None
        metadata["show_context_source"] = "request_context_rejected"
        metadata["show_context_repair_reason"] = stale_reason or "stale_request_context_mismatch"
        row["metadata"] = metadata

    for row in repair_rows:
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        title_id, title_url = _resolve_imdb_title_identity(row, metadata)
        if title_id:
            metadata["imdb_title_id"] = title_id
        if title_url:
            metadata["imdb_title_url"] = title_url

        imdb_title_type = str(metadata.get("imdb_title_type") or "").strip().upper()
        if not imdb_title_type and _has_imdb_episode_evidence_metadata(row, metadata):
            imdb_title_type = "TVEPISODE"
            metadata["imdb_title_type"] = imdb_title_type

        credit_media_type = _format_imdb_credit_media_type(imdb_title_type) or (
            str(metadata.get("imdb_credit_media_type") or "").strip() or None
        )
        if credit_media_type:
            metadata["imdb_credit_media_type"] = credit_media_type

        row["metadata"] = metadata

    repair_rows = [
        row
        for row in repair_rows
        if isinstance(row.get("source_image_id"), str) and str(row.get("source_image_id")).strip()
    ]
    if not repair_rows:
        if progress_cb:
            progress_cb(reviewed_rows, total_rows, changed_rows, metadata_failures)
        return 0, metadata_failures

    try:
        upserted = upsert_cast_photos(db, repair_rows, dedupe_on="source_image_id")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Existing IMDb repair upsert failed person_id=%s error=%s", person_id, exc)
        if progress_cb:
            progress_cb(reviewed_rows, total_rows, 0, metadata_failures + 1)
        return 0, metadata_failures + 1
    if progress_cb:
        progress_cb(total_rows, total_rows, len(upserted), metadata_failures)
    return len(upserted), metadata_failures


def _refresh_tmdb_profile(
    db: SupabaseAdminClient,
    person_id: str,
    *,
    tmdb_person_id: int | None,
) -> Literal["updated", "not_found", "skipped"]:
    def _canonicalize_link_url(value: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        parsed = urlparse(raw)
        if not parsed.scheme or not parsed.netloc:
            return raw
        scheme = parsed.scheme.lower()
        host = (parsed.hostname or "").lower()
        if not host:
            return raw
        netloc = host
        if parsed.port and not ((scheme == "http" and parsed.port == 80) or (scheme == "https" and parsed.port == 443)):
            netloc = f"{netloc}:{parsed.port}"
        path = parsed.path or "/"
        if path != "/":
            path = path.rstrip("/")
        return urlunparse((scheme, netloc, path, "", parsed.query, ""))

    def _url_key(value: str) -> str:
        return _canonicalize_link_url(value).lower()

    def _build_tmdb_social_links(person_name: str | None, external_ids: Any) -> list[dict[str, str]]:
        display_name = str(person_name or "").strip()
        out: list[dict[str, str]] = []

        def add(platform: str, label: str, raw_value: str | None) -> None:
            value = str(raw_value or "").strip().lstrip("@")
            if not value:
                return
            if platform == "twitter":
                url = f"https://x.com/{value}"
            elif platform == "instagram":
                url = f"https://www.instagram.com/{value}/"
            elif platform == "facebook":
                url = f"https://www.facebook.com/{value}"
            elif platform == "youtube":
                url = f"https://www.youtube.com/channel/{value}"
            elif platform == "tiktok":
                url = f"https://www.tiktok.com/@{value}"
            else:
                return
            canonical_url = _canonicalize_link_url(url)
            if not canonical_url:
                return
            out.append(
                {
                    "link_kind": platform,
                    "link_group": "social",
                    "label": f"{display_name} {label}".strip() if display_name else label,
                    "url": canonical_url,
                }
            )

        add("twitter", "Twitter/X", getattr(external_ids, "twitter_id", None))
        add("instagram", "Instagram", getattr(external_ids, "instagram_id", None))
        add("facebook", "Facebook", getattr(external_ids, "facebook_id", None))
        add("youtube", "YouTube", getattr(external_ids, "youtube_id", None))
        add("tiktok", "TikTok", getattr(external_ids, "tiktok_id", None))
        return out

    if not tmdb_person_id:
        return "skipped"
    from trr_backend.integrations.tmdb_person import fetch_tmdb_person_full
    from trr_backend.repositories.cast_tmdb import upsert_cast_tmdb

    person_full = fetch_tmdb_person_full(int(tmdb_person_id))
    if not person_full:
        return "not_found"
    upsert_cast_tmdb(db, person_full.to_cast_tmdb_row(person_id))

    try:
        social_rows = _build_tmdb_social_links(person_full.details.name, person_full.external_ids)
        if social_rows:
            show_cast_response = (
                db.schema("core").table("show_cast").select("show_id").eq("person_id", str(person_id)).execute()
            )
            if not (hasattr(show_cast_response, "error") and show_cast_response.error):
                show_ids = sorted(
                    {
                        str(item.get("show_id") or "").strip()
                        for item in (show_cast_response.data or [])
                        if isinstance(item, dict) and str(item.get("show_id") or "").strip()
                    }
                )
                for show_id in show_ids:
                    for row in social_rows:
                        payload = {
                            "show_id": show_id,
                            "entity_type": "person",
                            "entity_id": str(person_id),
                            "season_number": 0,
                            "link_group": row["link_group"],
                            "link_kind": row["link_kind"],
                            "label": row["label"],
                            "url": row["url"],
                            "url_key": _url_key(row["url"]),
                            "status": "approved",
                            "confidence": 0.95,
                            "source": "tmdb_profile_refresh",
                            "discovered_by": "tmdb_profile_refresh",
                            "metadata": {"source": "core.cast_tmdb"},
                            "created_by": "tmdb_profile_refresh",
                            "updated_by": "tmdb_profile_refresh",
                        }
                        db.schema("core").table("entity_links").upsert(
                            payload,
                            on_conflict="show_id,entity_type,entity_id,link_kind,season_number,url_key",
                        ).execute()
    except Exception as exc:  # noqa: BLE001
        logger.warning("TMDb social link sync failed for %s: %s", person_id, exc)
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
    stage_stats: dict[str, int] | None = None,
    failed_photo_ids: list[str] | None = None,
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
    normalized_photo_ids = _normalize_scope_ids(photo_ids)
    if photo_ids is not None and not normalized_photo_ids:
        _record_stage_row_stats(stage_stats, attempted_rows=0, skipped_existing_rows=0)
        return attempted, succeeded, unknown, failed

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
        if normalized_photo_ids:
            query = query.in_("id", normalized_photo_ids)
        response = query.execute()
        if hasattr(response, "error") and response.error:
            logger.warning("Word detection query failed for %s: %s", person_id, response.error)
            return attempted, succeeded, unknown, failed

        rows = response.data or []
        to_process: list[str] = []
        skipped_existing_rows = 0
        for row in rows:
            meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            if "has_text_overlay" in (meta or {}):
                skipped_existing_rows += 1
                continue
            rid = row.get("id")
            if rid:
                to_process.append(str(rid))

        total = len(to_process)
        _record_stage_row_stats(
            stage_stats,
            attempted_rows=total,
            skipped_existing_rows=skipped_existing_rows,
        )
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
                if failed_photo_ids is not None:
                    failed_photo_ids.append(photo_id)
                if reason_counts is not None:
                    reason = classify_text_overlay_failure_reason(exc)
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
                logger.warning("Word detection failed photo_id=%s error=%s", photo_id, exc)
            except Exception as exc:  # noqa: BLE001
                failed += 1
                if failed_photo_ids is not None:
                    failed_photo_ids.append(photo_id)
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
    stage_stats: dict[str, int] | None = None,
    failed_asset_ids: list[str] | None = None,
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
        skipped_existing_rows = 0
        for row in rows:
            asset_id = str(row.get("media_asset_id") or "")
            if not asset_id or asset_id in seen_asset_ids:
                continue
            if allowed_asset_ids and asset_id not in allowed_asset_ids:
                continue
            seen_asset_ids.add(asset_id)
            context = row.get("context") if isinstance(row.get("context"), dict) else {}
            metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            if "has_text_overlay" in context or "has_text_overlay" in metadata:
                skipped_existing_rows += 1
                continue
            to_process.append(asset_id)

        total = len(to_process)
        _record_stage_row_stats(
            stage_stats,
            attempted_rows=total,
            skipped_existing_rows=skipped_existing_rows,
        )
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
                if failed_asset_ids is not None:
                    failed_asset_ids.append(asset_id)
                if reason_counts is not None:
                    reason = classify_text_overlay_failure_reason(exc)
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
                logger.warning("Word detection failed media_asset_id=%s error=%s", asset_id, exc)
            except Exception as exc:  # noqa: BLE001
                failed += 1
                if failed_asset_ids is not None:
                    failed_asset_ids.append(asset_id)
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
    execution_profile = _resolve_execution_profile(request.execution_profile)

    # 1. Get person details
    person = _get_person_details(db, person_id_str)
    if not person:
        raise HTTPException(status_code=404, detail=f"Person {person_id} not found")

    external_ids = person.get("external_ids") or {}
    imdb_person_id = _extract_imdb_id(external_ids)
    tmdb_person_id = _get_tmdb_id(db, person_id_str, external_ids)
    person_name = person.get("full_name")
    show_name = request.show_name or _get_show_name(db, request.show_id)
    wwhl_credit_episode_imdb_ids = _load_person_wwhl_episode_imdb_ids_from_credits(db, person_id_str)
    sources, fandom_skipped = _resolve_refresh_sources(db, request)
    cast_photo_sources = [source for source in sources if source != "nbcumv"]
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
            sources=list(cast_photo_sources),
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
            episode_metadata_tagged, episode_metadata_failed = _enrich_cast_photos_with_episode_metadata(
                db,
                photos,
                person_wwhl_episode_imdb_ids=wwhl_credit_episode_imdb_ids,
            )
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
            wwhl_credit_episode_imdb_ids=wwhl_credit_episode_imdb_ids,
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
        mirror_parallelism = _resolve_stage_parallelism(
            request_overrides=request.max_parallelism,
            stage="mirror",
            default=_profile_default_parallelism(execution_profile, "mirror"),
        )
        mirror_batch_size = _resolve_stage_batch_size(
            request_overrides=request.batch_size,
            stage="mirror",
            default=_profile_default_batch_size(execution_profile, "mirror"),
        )
        try:
            cast_photos_mirrored, cast_photos_failed = _mirror_person_photos(
                db,
                person_id_str,
                imdb_person_id,
                force=request.force_mirror,
                max_parallelism=mirror_parallelism,
                batch_size=mirror_batch_size,
            )
            media_assets_mirrored, media_assets_failed = _mirror_person_media_assets(
                db,
                person_id_str,
                force=request.force_mirror,
                max_parallelism=mirror_parallelism,
                batch_size=mirror_batch_size,
            )
        except Exception as exc:
            logger.exception(f"Mirror error for {person_id}")
            errors.append(f"Mirror: {exc}")
    photos_mirrored = cast_photos_mirrored + media_assets_mirrored
    photos_failed = cast_photos_failed + media_assets_failed

    nbcumv_photos_fetched = 0
    nbcumv_assets_imported = 0
    nbcumv_assets_skipped = 0
    nbcumv_gallery_links_created = 0
    nbcumv_failed = 0
    getty_candidates_total = 0
    getty_matched_total = 0
    getty_unmatched_total = 0
    shared_nbcumv_total = 0
    shared_nbcumv_imported = 0
    nbcumv_only_total = 0
    nbcumv_only_imported = 0
    getty_only_imported = 0
    getty_snapshot_saved = False
    if "nbcumv" in sources:
        try:
            nbcumv_result = _import_nbcumv_person_media(
                db,
                person_id=person_id_str,
                person_name=person_name,
                show_id=request.show_id,
                show_name=show_name,
                limit=request.limit_per_source,
            )
            nbcumv_photos_fetched = int(nbcumv_result.get("fetched") or 0)
            nbcumv_assets_imported = int(nbcumv_result.get("imported") or 0)
            nbcumv_assets_skipped = int(nbcumv_result.get("skipped") or 0)
            nbcumv_gallery_links_created = int(nbcumv_result.get("gallery_links_created") or 0)
            nbcumv_failed = int(nbcumv_result.get("failed") or 0)
            getty_candidates_total = int(nbcumv_result.get("getty_candidates_total") or 0)
            getty_matched_total = int(nbcumv_result.get("getty_matched_total") or 0)
            getty_unmatched_total = int(nbcumv_result.get("getty_unmatched_total") or 0)
            shared_nbcumv_total = int(nbcumv_result.get("shared_nbcumv_total") or 0)
            shared_nbcumv_imported = int(nbcumv_result.get("shared_nbcumv_imported") or 0)
            nbcumv_only_total = int(nbcumv_result.get("nbcumv_only_total") or 0)
            nbcumv_only_imported = int(nbcumv_result.get("nbcumv_only_imported") or 0)
            getty_only_imported = int(nbcumv_result.get("getty_only_imported") or 0)
            getty_snapshot_saved = bool(nbcumv_result.get("getty_snapshot_saved"))
            errors.extend(
                [
                    str(error)
                    for error in (nbcumv_result.get("errors") or [])
                    if isinstance(error, str) and error.strip()
                ]
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("NBCUMV import stage failed for person_id=%s", person_id_str)
            nbcumv_failed += 1
            errors.append(f"NBCUMV: {exc}")

    if not request.skip_mirror and getty_only_imported > 0:
        try:
            getty_mirrored, getty_failed = _mirror_person_photos(
                db,
                person_id_str,
                imdb_person_id,
                force=request.force_mirror,
                max_parallelism=mirror_parallelism if "mirror_parallelism" in locals() else 12,
                batch_size=mirror_batch_size if "mirror_batch_size" in locals() else 200,
            )
            cast_photos_mirrored += getty_mirrored
            cast_photos_failed += getty_failed
            photos_mirrored += getty_mirrored
            photos_failed += getty_failed
        except Exception as exc:  # noqa: BLE001
            logger.exception("Getty fallback mirror failed for person_id=%s", person_id_str)
            errors.append(f"Getty mirror: {exc}")
            photos_failed += getty_only_imported

    photos_mirrored += nbcumv_assets_imported
    media_assets_mirrored += nbcumv_assets_imported
    photos_failed += nbcumv_failed
    photos_fetched_total = len(photos) + nbcumv_photos_fetched + getty_only_imported
    photos_upserted_total = photos_upserted + nbcumv_assets_imported + getty_only_imported

    # 4.5 Auto-count people for newly upserted TMDb/Fandom photos (only when no manual tags)
    auto_counts_attempted = 0
    auto_counts_succeeded = 0
    auto_counts_failed = 0
    auto_count_diagnostics = _empty_auto_count_diagnostics()
    auto_count_stage_stats = _empty_stage_row_stats()
    if not request.skip_auto_count:
        tagging_batch_size = _resolve_stage_batch_size(
            request_overrides=request.batch_size,
            stage="tagging",
            default=_profile_default_batch_size(execution_profile, "tagging"),
        )
        prefer_fast_pass = bool(request.prefer_fast_pass) if request.prefer_fast_pass is not None else True
        owner_reference_images: list[dict[str, Any]] = []
        owner_reference_synced = False

        try:
            owner_reference_profile = build_owner_tagging_reference_profile(
                db,
                person_id_str,
                show_id=request.show_id,
                show_name=request.show_name,
            )
            raw_refs = owner_reference_profile.get("used")
            if isinstance(raw_refs, list):
                owner_reference_images = [entry for entry in raw_refs if isinstance(entry, dict)]
            if owner_reference_images:
                owner_reference_images = sync_owner_tagging_reference_usage(
                    db,
                    person_id_str,
                    used_references=owner_reference_images,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to build owner tagging references for %s: %s", person_id_str, exc)
            owner_reference_images = []

        def _sync_owner_references_once(used_references: list[dict[str, Any]]) -> None:
            nonlocal owner_reference_images, owner_reference_synced
            if owner_reference_synced:
                return
            owner_reference_images = sync_owner_tagging_reference_usage(
                db,
                person_id_str,
                used_references=used_references,
            )
            owner_reference_synced = True

        auto_counts_attempted_cast, auto_counts_succeeded_cast, auto_counts_failed_cast = _auto_count_cast_photos(
            db,
            person_id_str,
            sources,
            owner_person_name=person_name,
            owner_reference_images=owner_reference_images,
            owner_reference_sync_cb=_sync_owner_references_once,
            photo_ids=None,
            request_show_id=request.show_id,
            request_show_name=request.show_name,
            diagnostics=auto_count_diagnostics,
            stage_stats=auto_count_stage_stats,
            tagging_batch_size=tagging_batch_size,
            prefer_fast_pass=prefer_fast_pass,
        )
        auto_counts_attempted_media, auto_counts_succeeded_media, auto_counts_failed_media = _auto_count_media_links(
            db,
            person_id_str,
            owner_person_name=person_name,
            owner_reference_images=owner_reference_images,
            owner_reference_sync_cb=_sync_owner_references_once,
            force_recount=False,
            request_show_id=request.show_id,
            request_show_name=request.show_name,
            diagnostics=auto_count_diagnostics,
            stage_stats=auto_count_stage_stats,
            tagging_batch_size=tagging_batch_size,
            prefer_fast_pass=prefer_fast_pass,
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
    text_overlay_stage_stats = _empty_stage_row_stats()
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
            stage_stats=text_overlay_stage_stats,
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
            stage_stats=text_overlay_stage_stats,
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
        crop_parallelism = _resolve_stage_parallelism(
            request_overrides=request.max_parallelism,
            stage="crop",
            default=_profile_default_parallelism(execution_profile, "crop"),
        )
        centering_attempted, centering_succeeded, centering_failed, centering_skipped_manual = (
            _recenter_person_gallery_images(
                db,
                person_id_str,
                sources,
                photo_ids=None,
                force=False,
                max_parallelism=crop_parallelism,
                owner_person_name=person_name,
                prefer_fast_pass=bool(request.prefer_fast_pass),
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

    row_error_counts = _build_auto_count_row_error_counts(auto_count_diagnostics)
    failed_parts = _build_failed_parts_summary(
        metadata_enrichment_failed=metadata_enrichment_failed,
        auto_counts_failed=auto_counts_failed,
        row_error_counts=row_error_counts,
        text_overlay_failed=text_overlay_failed,
        text_overlay_failure_reasons=text_overlay_failure_reasons,
        centering_failed=centering_failed,
        resize_failed=resize_failed,
        resize_crop_failed=resize_crop_failed,
    )

    return RefreshImagesResponse(
        person_id=person_id_str,
        person_name=person_name,
        imdb_person_id=imdb_person_id,
        tmdb_person_id=tmdb_person_id,
        tmdb_profile_status=tmdb_profile_status,
        tmdb_profile_error_code=tmdb_profile_error_code,
        tmdb_profile_error_detail=tmdb_profile_error_detail,
        sources_used=list(sources),
        photos_fetched=photos_fetched_total,
        photos_upserted=photos_upserted_total,
        photos_mirrored=photos_mirrored,
        photos_failed=photos_failed,
        cast_photos_mirrored=cast_photos_mirrored,
        cast_photos_failed=cast_photos_failed,
        media_assets_mirrored=media_assets_mirrored,
        media_assets_failed=media_assets_failed,
        nbcumv_photos_fetched=nbcumv_photos_fetched,
        nbcumv_assets_imported=nbcumv_assets_imported,
        nbcumv_assets_skipped=nbcumv_assets_skipped,
        nbcumv_gallery_links_created=nbcumv_gallery_links_created,
        nbcumv_failed=nbcumv_failed,
        getty_candidates_total=getty_candidates_total,
        getty_matched_total=getty_matched_total,
        getty_unmatched_total=getty_unmatched_total,
        shared_nbcumv_total=shared_nbcumv_total,
        shared_nbcumv_imported=shared_nbcumv_imported,
        nbcumv_only_total=nbcumv_only_total,
        nbcumv_only_imported=nbcumv_only_imported,
        getty_only_imported=getty_only_imported,
        getty_snapshot_saved=getty_snapshot_saved,
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
        auto_count_attempted_rows=int(auto_count_stage_stats.get("attempted_rows", 0)),
        auto_count_skipped_existing_rows=int(auto_count_stage_stats.get("skipped_existing_rows", 0)),
        auto_count_retry_attempted_rows=0,
        auto_count_retry_succeeded_rows=0,
        auto_faces_detected=int(auto_count_diagnostics.get("auto_faces_detected", 0)),
        auto_face_crops_generated=int(auto_count_diagnostics.get("auto_face_crops_generated", 0)),
        auto_person_fallback_crops_generated=int(auto_count_diagnostics.get("auto_person_fallback_crops_generated", 0)),
        auto_no_face_rows=int(auto_count_diagnostics.get("auto_no_face_rows", 0)),
        auto_identity_skipped_non_trr_show=int(auto_count_diagnostics.get("auto_identity_skipped_non_trr_show", 0)),
        auto_detect_success_rows=int(auto_count_diagnostics.get("auto_detect_success_rows", 0)),
        auto_detect_failed_rows=int(auto_count_diagnostics.get("auto_detect_failed_rows", 0)),
        auto_persist_success_rows=int(auto_count_diagnostics.get("auto_persist_success_rows", 0)),
        auto_persist_failed_rows=int(auto_count_diagnostics.get("auto_persist_failed_rows", 0)),
        auto_crop_cache_success_rows=int(auto_count_diagnostics.get("auto_crop_cache_success_rows", 0)),
        auto_crop_cache_failed_rows=int(auto_count_diagnostics.get("auto_crop_cache_failed_rows", 0)),
        row_error_counts=row_error_counts,
        text_overlay_attempted=text_overlay_attempted,
        text_overlay_succeeded=text_overlay_succeeded,
        text_overlay_unknown=text_overlay_unknown,
        text_overlay_failed=text_overlay_failed,
        text_overlay_attempted_rows=int(text_overlay_stage_stats.get("attempted_rows", 0)),
        text_overlay_skipped_existing_rows=int(text_overlay_stage_stats.get("skipped_existing_rows", 0)),
        text_overlay_retry_attempted_rows=0,
        text_overlay_retry_succeeded_rows=0,
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
        retry_attempts={
            "auto_count": 1,
            "word_id": 1,
            "centering_cropping": 1,
            "resizing": 1,
        },
        failed_parts=failed_parts,
        errors=errors,
    )


@router.post("/{person_id}/refresh-images/stream")
async def refresh_person_images_stream(
    person_id: UUID,
    connection: Request,
    request: RefreshImagesRequest | None = None,
    db: SupabaseAdminClient = None,
    admin_user: AdminUser = None,
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
    operation_id = "operation-pending"
    request_id = str(connection.headers.get("x-trr-request-id") or "").strip() or None
    execution_profile = _resolve_execution_profile(request.execution_profile)
    sync_parallelism = _resolve_stage_parallelism(
        request_overrides=request.max_parallelism,
        stage="sync",
        default=_profile_default_parallelism(execution_profile, "sync"),
    )

    async def event_generator() -> AsyncGenerator[str, None]:
        event_seq = 0
        errors: list[str] = []
        upserted_photo_ids: list[str] = []
        text_overlay_reason_counts: dict[str, int] = dict.fromkeys(TEXT_OVERLAY_FAILURE_REASONS, 0)
        episode_metadata_tagged = 0
        show_context_tagged = 0
        metadata_enrichment_failed = 0
        photos_upserted = 0
        existing_imdb_rows_repaired = 0
        imdb_diagnostics = _empty_imdb_refresh_diagnostics()
        wwhl_credit_episode_imdb_ids: set[str] = set()
        photos_mirrored = 0
        cast_photos_mirrored = 0
        media_assets_mirrored = 0
        nbcumv_photos_fetched = 0
        nbcumv_assets_imported = 0
        nbcumv_assets_skipped = 0
        nbcumv_gallery_links_created = 0
        nbcumv_failed = 0
        getty_candidates_total = 0
        getty_matched_total = 0
        getty_unmatched_total = 0
        shared_nbcumv_total = 0
        shared_nbcumv_imported = 0
        nbcumv_only_total = 0
        nbcumv_only_imported = 0
        getty_only_imported = 0
        getty_snapshot_saved = False
        auto_counts_succeeded = 0
        auto_count_diagnostics = _empty_auto_count_diagnostics()
        auto_count_stage_stats = _empty_stage_row_stats()
        text_overlay_succeeded = 0
        text_overlay_stage_stats = _empty_stage_row_stats()
        centering_succeeded = 0
        resize_succeeded = 0
        tmdb_profile_status: Literal["ok", "skipped", "failed"] | None = None
        tmdb_profile_error_code: str | None = None
        tmdb_profile_error_detail: str | None = None
        source_progress: dict[str, dict[str, Any]] = {}
        source_progress_lock = Lock()
        source_progress_unset = object()

        def update_source_progress(
            source_key: str | None,
            *,
            discovered_total: int | None | object = source_progress_unset,
            scraped_current: int | None | object = source_progress_unset,
            saved_current: int | None | object = source_progress_unset,
            failed_current: int | None | object = source_progress_unset,
            skipped_current: int | None | object = source_progress_unset,
            remaining: int | None | object = source_progress_unset,
            status: SourceProgressStatus | object = source_progress_unset,
            message: str | None | object = source_progress_unset,
        ) -> None:
            normalized_key = _normalize_source_progress_key(source_key)
            if normalized_key is None:
                return
            with source_progress_lock:
                entry = source_progress.setdefault(normalized_key, _empty_source_progress_entry())
                if discovered_total is not source_progress_unset:
                    entry["discovered_total"] = (
                        max(0, int(discovered_total)) if isinstance(discovered_total, int) else None
                    )
                if scraped_current is not source_progress_unset:
                    entry["scraped_current"] = max(0, int(scraped_current or 0))
                if saved_current is not source_progress_unset:
                    entry["saved_current"] = max(0, int(saved_current or 0))
                if failed_current is not source_progress_unset:
                    entry["failed_current"] = max(0, int(failed_current or 0))
                if skipped_current is not source_progress_unset:
                    entry["skipped_current"] = max(0, int(skipped_current or 0))
                if remaining is not source_progress_unset:
                    entry["remaining"] = max(0, int(remaining)) if isinstance(remaining, int) else None
                elif isinstance(entry.get("discovered_total"), int):
                    entry["remaining"] = max(
                        0,
                        int(entry["discovered_total"]) - int(entry.get("scraped_current") or 0),
                    )
                if status is not source_progress_unset:
                    entry["status"] = cast(SourceProgressStatus, status)
                if message is not source_progress_unset:
                    entry["message"] = str(message).strip() if isinstance(message, str) and message.strip() else None

        def source_progress_snapshot() -> dict[str, dict[str, Any]]:
            with source_progress_lock:
                snapshot = {key: dict(value) for key, value in source_progress.items()}
            return _ordered_source_progress_snapshot(snapshot)

        def build_live_counts() -> dict[str, int]:
            return {
                "synced": int(photos_upserted),
                "mirrored": int(photos_mirrored),
                "counted": int(auto_counts_succeeded),
                "cropped": int(centering_succeeded),
                "id_text": int(text_overlay_succeeded),
                "resized": int(resize_succeeded),
            }

        def envelope(payload: dict[str, Any]) -> dict[str, Any]:
            nonlocal event_seq
            event_seq += 1
            return {
                "operation_id": operation_id,
                "event_seq": event_seq,
                **payload,
            }

        def progress(payload: dict[str, Any]) -> str:
            return (
                "event: progress\ndata: "
                + json.dumps(
                    envelope(
                        {
                            "run_id": run_id,
                            "live_counts": build_live_counts(),
                            "source_progress": source_progress_snapshot(),
                            **payload,
                        }
                    )
                )
                + "\n\n"
            )

        def error_event(
            *,
            stage: str,
            error: str,
            detail: str | None = None,
            stage_error_code: str | None = None,
            stage_error_detail: str | None = None,
        ) -> str:
            payload: dict[str, Any] = {"run_id": run_id, "stage": stage, "error": error}
            if detail:
                payload["detail"] = detail
            if stage_error_code:
                payload["stage_error_code"] = stage_error_code
            if stage_error_detail:
                payload["stage_error_detail"] = stage_error_detail
            return f"event: error\ndata: {json.dumps(envelope(payload))}\n\n"

        async def _client_disconnected(stage: str) -> bool:
            try:
                disconnected = await connection.is_disconnected()
            except Exception:  # noqa: BLE001
                disconnected = False
            if disconnected:
                logger.info("Refresh stream client disconnected person_id=%s stage=%s", person_id_str, stage)
            return disconnected

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
        if await _client_disconnected("starting"):
            return

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
            show_name = request.show_name or await asyncio.to_thread(_get_show_name, db, request.show_id)
            wwhl_credit_episode_imdb_ids = await asyncio.to_thread(
                _load_person_wwhl_episode_imdb_ids_from_credits,
                db,
                person_id_str,
            )
            requested_sources = list(request.sources or ALL_SOURCES)
            sources, fandom_skipped = await asyncio.to_thread(_resolve_refresh_sources, db, request)
            if fandom_skipped:
                errors.append("Fandom sources skipped for non-Real Housewives show context.")
        except Exception as exc:  # noqa: BLE001
            logger.exception("Refresh stream setup failed for %s: %s", person_id_str, exc)
            yield error_event(
                stage="setup",
                error="Failed to initialize refresh",
                detail=str(exc),
                stage_error_code="REFRESH_SETUP_FAILED",
                stage_error_detail=str(exc),
            )
            return

        for requested_source in requested_sources:
            update_source_progress(requested_source, status="pending", message="Pending")

        for filtered_source in set(requested_sources) - set(sources):
            update_source_progress(
                filtered_source,
                status="skipped",
                remaining=0,
                message="Skipped by source policy.",
            )
        if "imdb" in requested_sources and not imdb_person_id:
            update_source_progress("imdb", status="skipped", remaining=0, message="No IMDb person ID.")
        if "tmdb" in requested_sources and not tmdb_person_id:
            update_source_progress("tmdb", status="skipped", remaining=0, message="No TMDb person ID.")
        if "fandom" in requested_sources and not person_name:
            update_source_progress("fandom", status="skipped", remaining=0, message="No person name.")
        if "fandom-gallery" in requested_sources and not person_name:
            update_source_progress("fandom-gallery", status="skipped", remaining=0, message="No person name.")

        # 1.5 Refresh person profiles (best-effort)
        if await _client_disconnected("tmdb_profile"):
            return
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
            if await _client_disconnected("fandom_profile"):
                return
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

        async def _run_source_fetch_task(
            *,
            stage_name: str,
            person_identifier: str,
            imdb_identifier: str | None,
            tmdb_identifier: int | None,
            name: str,
            limit: int,
            allowed_title_imdb_ids: set[str],
            allowed_title_keywords: list[str],
            prioritize_solo: bool,
            strict_context: dict[str, Any],
        ) -> list[dict[str, Any]]:
            def _run_sync() -> list[dict[str, Any]]:
                if stage_name == "sync_imdb":
                    return fetch_imdb_cast_photos(
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
                        imdb_diagnostics=imdb_diagnostics,
                        session=None,
                        verbose=False,
                    )
                if stage_name == "sync_tmdb":
                    return fetch_tmdb_cast_photos(
                        int(tmdb_identifier),
                        person_identifier,
                        imdb_person_id=imdb_identifier,
                        limit=limit,
                        verbose=False,
                    )
                if stage_name == "sync_fandom":
                    return fetch_fandom_person_cast_photos(
                        name,
                        person_identifier,
                        imdb_person_id=imdb_identifier,
                        limit=limit,
                        verbose=False,
                    )
                return fetch_fandom_gallery_cast_photos(
                    name,
                    person_identifier,
                    imdb_person_id=imdb_identifier,
                    limit=limit,
                    verbose=False,
                )

            return await asyncio.to_thread(_run_sync)

        fetch_entries: list[dict[str, Any]] = []
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
            fetch_entries.append(
                {
                    "stage": stage,
                    "label": label,
                    "source_name": source_name,
                    "source_total": source_total,
                    "mirrored_count": mirrored_count,
                    "started_at": time.perf_counter(),
                }
            )
            update_source_progress(
                source_name,
                status="running",
                discovered_total=None,
                scraped_current=0,
                remaining=None,
                message=f"Syncing {label}...",
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

        pending_tasks: dict[asyncio.Task[list[dict[str, Any]]], dict[str, Any]] = {}
        semaphore = asyncio.Semaphore(max(1, int(sync_parallelism)))

        async def _run_entry(entry: dict[str, Any]) -> list[dict[str, Any]]:
            async with semaphore:
                return await _run_source_fetch_task(
                    stage_name=str(entry["stage"]),
                    person_identifier=person_id_str,
                    imdb_identifier=imdb_person_id,
                    tmdb_identifier=tmdb_person_id,
                    name=str(person_name),
                    limit=request.limit_per_source,
                    allowed_title_imdb_ids=set(imdb_allowed_title_ids),
                    allowed_title_keywords=list(imdb_allowed_keywords),
                    prioritize_solo=imdb_prioritize_solo,
                    strict_context=imdb_strict_context,
                )

        for entry in fetch_entries:
            task = asyncio.create_task(_run_entry(entry))
            pending_tasks[task] = entry

        while pending_tasks:
            if await _client_disconnected("sync_fetch"):
                for task in list(pending_tasks):
                    task.cancel()
                return
            done, _ = await asyncio.wait(
                set(pending_tasks.keys()),
                timeout=2.0,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                yield progress(
                    {
                        "stage": "fetching",
                        "message": "Syncing sources...",
                        "current": processed_sources,
                        "total": total_sources,
                        "heartbeat": True,
                    }
                )
                continue

            for task in done:
                entry = pending_tasks.pop(task)
                stage = str(entry["stage"])
                label = str(entry["label"])
                source_name = cast(SourceType | None, entry.get("source_name"))
                source_total = entry.get("source_total")
                mirrored_count = entry.get("mirrored_count")
                rows: list[dict[str, Any]] = []
                try:
                    rows = task.result()
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"{label}: {exc}")
                    update_source_progress(
                        source_name,
                        status="failed",
                        failed_current=1,
                        message=f"{label} sync failed: {exc}",
                    )
                    rows = []
                photos.extend(rows)
                processed_sources += 1
                elapsed_ms = int((time.perf_counter() - float(entry.get("started_at") or time.perf_counter())) * 1000)
                update_source_progress(
                    source_name,
                    status="completed",
                    discovered_total=len(rows),
                    scraped_current=len(rows),
                    remaining=0,
                    message=f"Synced {label} ({len(rows)} photos).",
                )
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
                _enrich_cast_photos_with_episode_metadata,
                db,
                photos,
                person_wwhl_episode_imdb_ids=wwhl_credit_episode_imdb_ids,
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
            per_source_photo_counts = Counter(
                _normalize_source_progress_key(str(photo.get("source") or ""))
                for photo in photos
                if _normalize_source_progress_key(str(photo.get("source") or ""))
            )
            try:
                if imdb_photos:
                    upserted = await asyncio.to_thread(upsert_cast_photos, db, imdb_photos, dedupe_on="source_image_id")
                    photos_upserted += len(upserted)
                    upserted_photo_ids.extend([str(row["id"]) for row in upserted if row.get("id")])
                if other_photos:
                    upserted = await asyncio.to_thread(
                        upsert_cast_photos, db, other_photos, dedupe_on="image_url_canonical"
                    )
                    photos_upserted += len(upserted)
                    upserted_photo_ids.extend([str(row["id"]) for row in upserted if row.get("id")])
                for source_key, count in per_source_photo_counts.items():
                    update_source_progress(
                        source_key,
                        saved_current=int(count),
                        message=(
                            f"Saved {int(count)} photo{'s' if int(count) != 1 else ''}."
                            if int(count) > 0
                            else "No photos saved."
                        ),
                    )
            except Exception as exc:
                errors.append(str(exc))
                for source_key, count in per_source_photo_counts.items():
                    update_source_progress(
                        source_key,
                        failed_current=int(count),
                        message=f"Save failed: {exc}",
                        status="failed",
                    )
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
        metadata_repair_progress = {
            "reviewed_rows": 0,
            "changed_rows": 0,
            "total_rows": 0,
            "failed_rows": 0,
        }
        metadata_repair_lock = Lock()

        def _update_metadata_repair_progress(
            reviewed_rows: int,
            total_rows: int,
            changed_rows: int,
            failed_rows: int,
        ) -> None:
            with metadata_repair_lock:
                metadata_repair_progress["reviewed_rows"] = max(0, int(reviewed_rows))
                metadata_repair_progress["total_rows"] = max(0, int(total_rows))
                metadata_repair_progress["changed_rows"] = max(0, int(changed_rows))
                metadata_repair_progress["failed_rows"] = max(0, int(failed_rows))

        yield progress(
            {
                "stage": "metadata_repair",
                "message": "Fixing IMDb Details...",
                "current": 0,
                "total": 0,
                "reviewed_rows": 0,
                "changed_rows": 0,
                "total_rows": 0,
                "failed_rows": 0,
            }
        )
        try:
            metadata_repair_started_at = time.perf_counter()
            metadata_repair_task = asyncio.create_task(
                asyncio.to_thread(
                    _repair_existing_imdb_cast_photos,
                    db,
                    person_id_str,
                    show_id=request.show_id,
                    show_name=request.show_name,
                    strict_context=imdb_strict_context,
                    wwhl_credit_episode_imdb_ids=wwhl_credit_episode_imdb_ids,
                    progress_cb=_update_metadata_repair_progress,
                )
            )
            while not metadata_repair_task.done():
                await asyncio.sleep(2)
                if metadata_repair_task.done():
                    break
                if await _client_disconnected("metadata_repair"):
                    metadata_repair_task.cancel()
                    return
                with metadata_repair_lock:
                    metadata_repair_snapshot = dict(metadata_repair_progress)
                total_rows = int(metadata_repair_snapshot.get("total_rows", 0))
                reviewed_rows = int(metadata_repair_snapshot.get("reviewed_rows", 0))
                yield progress(
                    {
                        "stage": "metadata_repair",
                        "message": "Fixing IMDb Details...",
                        "current": reviewed_rows,
                        "total": total_rows,
                        "reviewed_rows": reviewed_rows,
                        "changed_rows": int(metadata_repair_snapshot.get("changed_rows", 0)),
                        "total_rows": total_rows,
                        "failed_rows": int(metadata_repair_snapshot.get("failed_rows", 0)),
                        "heartbeat": True,
                        "elapsed_ms": int((time.perf_counter() - metadata_repair_started_at) * 1000),
                    }
                )
            existing_imdb_rows_repaired, repair_failed = await metadata_repair_task
            metadata_enrichment_failed += repair_failed
            with metadata_repair_lock:
                metadata_repair_snapshot = dict(metadata_repair_progress)
            reviewed_rows = int(metadata_repair_snapshot.get("reviewed_rows", 0))
            total_rows = int(metadata_repair_snapshot.get("total_rows", 0))
            changed_rows = int(metadata_repair_snapshot.get("changed_rows", 0))
            failed_rows = int(metadata_repair_snapshot.get("failed_rows", 0))
            yield progress(
                {
                    "stage": "metadata_repair",
                    "message": (
                        "Fixing IMDb Details complete "
                        f"(reviewed {reviewed_rows}/{total_rows}, "
                        f"changed {changed_rows}, failed {failed_rows})."
                    ),
                    "current": reviewed_rows,
                    "total": total_rows,
                    "reviewed_rows": reviewed_rows,
                    "changed_rows": changed_rows,
                    "total_rows": total_rows,
                    "failed_rows": failed_rows,
                }
            )
        except Exception as exc:  # noqa: BLE001
            metadata_enrichment_failed += 1
            errors.append(f"Metadata repair: {exc}")
            with metadata_repair_lock:
                metadata_repair_snapshot = dict(metadata_repair_progress)
            reviewed_rows = int(metadata_repair_snapshot.get("reviewed_rows", 0))
            total_rows = int(metadata_repair_snapshot.get("total_rows", 0))
            yield progress(
                {
                    "stage": "metadata_repair",
                    "message": f"Fixing IMDb Details failed: {exc}",
                    "current": reviewed_rows,
                    "total": total_rows,
                    "reviewed_rows": reviewed_rows,
                    "changed_rows": int(metadata_repair_snapshot.get("changed_rows", 0)),
                    "total_rows": total_rows,
                    "failed_rows": int(metadata_repair_snapshot.get("failed_rows", 0)),
                }
            )

        # 4. Mirror
        cast_photos_mirrored, cast_photos_failed = 0, 0
        media_assets_mirrored, media_assets_failed = 0, 0
        if not request.skip_mirror:
            mirror_parallelism = _resolve_stage_parallelism(
                request_overrides=request.max_parallelism,
                stage="mirror",
                default=_profile_default_parallelism(execution_profile, "mirror"),
            )
            mirror_batch_size = _resolve_stage_batch_size(
                request_overrides=request.batch_size,
                stage="mirror",
                default=_profile_default_batch_size(execution_profile, "mirror"),
            )
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
                    max_parallelism=mirror_parallelism,
                    batch_size=mirror_batch_size,
                )
                yield progress(
                    {
                        "stage": "mirroring",
                        "message": (
                            f"Mirrored cast photos ({cast_photos_mirrored} hosted"
                            + (f", {cast_photos_failed} failed" if cast_photos_failed > 0 else "")
                            + ")."
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
                    max_parallelism=mirror_parallelism,
                    batch_size=mirror_batch_size,
                )
                yield progress(
                    {
                        "stage": "mirroring",
                        "message": (
                            f"Mirrored media assets ({media_assets_mirrored} hosted"
                            + (f", {media_assets_failed} failed" if media_assets_failed > 0 else "")
                            + ")."
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

        if "nbcumv" in sources:
            if await _client_disconnected("nbcumv_import"):
                return
            nbcumv_progress = {
                "current": 0,
                "total": 0,
                "message": "Importing NBCUMV press photos...",
            }
            nbcumv_progress_events: list[dict[str, Any]] = []
            nbcumv_progress_lock = Lock()

            def _update_nbcumv_progress(current: int, total: int, message: str) -> None:
                snapshot = {
                    "current": max(0, int(current)),
                    "total": max(0, int(total)),
                    "message": str(message or "Importing NBCUMV press photos...").strip(),
                }
                with nbcumv_progress_lock:
                    nbcumv_progress.update(snapshot)
                    nbcumv_progress_events.append(snapshot)
                update_source_progress(
                    "nbcumv",
                    status="running",
                    discovered_total=snapshot["total"] if snapshot["total"] > 0 else None,
                    scraped_current=snapshot["current"],
                    remaining=max(0, snapshot["total"] - snapshot["current"]) if snapshot["total"] > 0 else 0,
                    message=snapshot["message"],
                )

            update_source_progress("nbcumv", status="running", message="Importing NBCUMV press photos...")
            yield progress(
                {
                    "stage": "nbcumv_import",
                    "message": "Importing NBCUMV press photos...",
                    "current": 0,
                    "total": 0,
                }
            )
            try:
                nbcumv_started_at = time.perf_counter()
                nbcumv_task = asyncio.create_task(
                    asyncio.to_thread(
                        _import_nbcumv_person_media,
                        db,
                        person_id=person_id_str,
                        person_name=person_name,
                        show_id=request.show_id,
                        show_name=show_name,
                        limit=request.limit_per_source,
                        progress_cb=_update_nbcumv_progress,
                    )
                )
                while not nbcumv_task.done():
                    await asyncio.sleep(2)
                    if nbcumv_task.done():
                        break
                    if await _client_disconnected("nbcumv_import"):
                        nbcumv_task.cancel()
                        return
                    with nbcumv_progress_lock:
                        nbcumv_snapshot = dict(nbcumv_progress)
                        nbcumv_pending_events = list(nbcumv_progress_events)
                        nbcumv_progress_events.clear()
                    for pending_event in nbcumv_pending_events:
                        yield progress(
                            {
                                "stage": "nbcumv_import",
                                "message": str(pending_event.get("message") or "Importing NBCUMV press photos..."),
                                "current": int(pending_event.get("current") or 0),
                                "total": int(pending_event.get("total") or 0),
                            }
                        )
                    yield progress(
                        {
                            "stage": "nbcumv_import",
                            "message": str(nbcumv_snapshot.get("message") or "Importing NBCUMV press photos..."),
                            "current": int(nbcumv_snapshot.get("current") or 0),
                            "total": int(nbcumv_snapshot.get("total") or 0),
                            "heartbeat": True,
                            "elapsed_ms": int((time.perf_counter() - nbcumv_started_at) * 1000),
                        }
                    )
                nbcumv_result = await nbcumv_task
                nbcumv_photos_fetched = int(nbcumv_result.get("fetched") or 0)
                nbcumv_assets_imported = int(nbcumv_result.get("imported") or 0)
                nbcumv_assets_skipped = int(nbcumv_result.get("skipped") or 0)
                nbcumv_gallery_links_created = int(nbcumv_result.get("gallery_links_created") or 0)
                nbcumv_failed = int(nbcumv_result.get("failed") or 0)
                getty_candidates_total = int(nbcumv_result.get("getty_candidates_total") or 0)
                getty_matched_total = int(nbcumv_result.get("getty_matched_total") or 0)
                getty_unmatched_total = int(nbcumv_result.get("getty_unmatched_total") or 0)
                shared_nbcumv_total = int(nbcumv_result.get("shared_nbcumv_total") or 0)
                shared_nbcumv_imported = int(nbcumv_result.get("shared_nbcumv_imported") or 0)
                nbcumv_only_total = int(nbcumv_result.get("nbcumv_only_total") or 0)
                nbcumv_only_imported = int(nbcumv_result.get("nbcumv_only_imported") or 0)
                getty_only_imported = int(nbcumv_result.get("getty_only_imported") or 0)
                getty_snapshot_saved = bool(nbcumv_result.get("getty_snapshot_saved"))
                nbcumv_summary_message = str(nbcumv_result.get("summary_message") or "").strip()
                errors.extend(
                    [
                        str(error)
                        for error in (nbcumv_result.get("errors") or [])
                        if isinstance(error, str) and error.strip()
                    ]
                )
                with nbcumv_progress_lock:
                    nbcumv_snapshot = dict(nbcumv_progress)
                    nbcumv_pending_events = list(nbcumv_progress_events)
                    nbcumv_progress_events.clear()
                for pending_event in nbcumv_pending_events:
                    yield progress(
                        {
                            "stage": "nbcumv_import",
                            "message": str(pending_event.get("message") or "Importing NBCUMV press photos..."),
                            "current": int(pending_event.get("current") or 0),
                            "total": int(pending_event.get("total") or 0),
                        }
                    )
                photos_upserted += nbcumv_assets_imported + getty_only_imported
                photos_mirrored += nbcumv_assets_imported
                media_assets_mirrored += nbcumv_assets_imported
                photos_failed += nbcumv_failed
                if not request.skip_mirror and getty_only_imported > 0:
                    yield progress(
                        {
                            "stage": "nbcumv_import",
                            "message": f"Mirroring {getty_only_imported} Getty-only fallback photos...",
                            "current": int(nbcumv_snapshot.get("current") or 0),
                            "total": int(nbcumv_snapshot.get("total") or 0),
                        }
                    )
                    try:
                        getty_mirrored, getty_failed = await asyncio.to_thread(
                            _mirror_person_photos,
                            db,
                            person_id_str,
                            imdb_person_id,
                            force=request.force_mirror,
                            max_parallelism=mirror_parallelism,
                            batch_size=mirror_batch_size,
                        )
                        cast_photos_mirrored += getty_mirrored
                        cast_photos_failed += getty_failed
                        photos_mirrored += getty_mirrored
                        photos_failed += getty_failed
                        yield progress(
                            {
                                "stage": "nbcumv_import",
                                "message": (
                                    f"Mirrored Getty-only fallback photos ({getty_mirrored} hosted"
                                    + (f", {getty_failed} failed" if getty_failed > 0 else "")
                                    + ")."
                                ),
                                "current": int(nbcumv_snapshot.get("current") or 0),
                                "total": int(nbcumv_snapshot.get("total") or 0),
                            }
                        )
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"Getty mirror: {exc}")
                        yield progress(
                            {
                                "stage": "nbcumv_import",
                                "message": f"Getty fallback mirroring failed: {exc}",
                                "current": int(nbcumv_snapshot.get("current") or 0),
                                "total": int(nbcumv_snapshot.get("total") or 0),
                            }
                        )
                yield progress(
                    {
                        "stage": "nbcumv_import",
                        "message": (
                            nbcumv_summary_message
                            or (
                                f"{str(nbcumv_snapshot.get('message') or 'NBCUMV import complete')}. "
                                f"Summary: {shared_nbcumv_imported} shared via NBCUMV, "
                                f"{nbcumv_only_imported} NBCUMV-only, "
                                f"{getty_only_imported} Getty-only, {nbcumv_assets_skipped} skipped, "
                                f"{nbcumv_failed} failed."
                            )
                        ),
                        "current": max(
                            int(nbcumv_snapshot.get("current") or 0),
                            nbcumv_photos_fetched + nbcumv_assets_skipped + nbcumv_failed,
                            getty_candidates_total,
                        ),
                        "total": max(
                            int(nbcumv_snapshot.get("total") or 0),
                            nbcumv_photos_fetched + nbcumv_assets_skipped + nbcumv_failed,
                            getty_candidates_total,
                        ),
                    }
                )
                update_source_progress(
                    "nbcumv",
                    status="failed" if nbcumv_failed > 0 else "completed",
                    discovered_total=max(
                        int(nbcumv_snapshot.get("total") or 0),
                        nbcumv_photos_fetched + nbcumv_assets_skipped + nbcumv_failed,
                        getty_candidates_total,
                    ),
                    scraped_current=max(
                        int(nbcumv_snapshot.get("current") or 0),
                        nbcumv_photos_fetched + nbcumv_assets_skipped + nbcumv_failed,
                        getty_candidates_total,
                    ),
                    saved_current=nbcumv_assets_imported + getty_only_imported,
                    failed_current=nbcumv_failed,
                    skipped_current=nbcumv_assets_skipped,
                    remaining=0,
                    message=(
                        nbcumv_summary_message
                        or (
                            f"Getty/NBCUMV complete: {shared_nbcumv_imported} shared via NBCUMV, "
                            f"{nbcumv_only_imported} NBCUMV-only, {getty_only_imported} Getty-only, "
                            f"{nbcumv_assets_skipped} skipped, {nbcumv_failed} failed."
                        )
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("NBCUMV stream import failed for person_id=%s", person_id_str)
                nbcumv_failed += 1
                photos_failed += 1
                errors.append(f"NBCUMV: {exc}")
                with nbcumv_progress_lock:
                    nbcumv_snapshot = dict(nbcumv_progress)
                update_source_progress(
                    "nbcumv",
                    status="failed",
                    failed_current=nbcumv_failed,
                    message=f"NBCUMV import failed: {exc}",
                )
                yield progress(
                    {
                        "stage": "nbcumv_import",
                        "message": f"NBCUMV import failed: {exc}",
                        "current": int(nbcumv_snapshot.get("current") or 0),
                        "total": int(nbcumv_snapshot.get("total") or 0),
                    }
                )

        # 5. Prune
        photos_pruned = 0
        if not request.skip_mirror and not request.skip_prune:
            yield progress({"stage": "pruning", "message": "Pruning orphaned S3 objects..."})
            photos_pruned = await asyncio.to_thread(_prune_person_s3_objects, db, imdb_person_id or person_id_str)
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
                                "message": "Skipping auto-count (vision unavailable).",
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
                                    "source_page_url, people_names, title_names, caption, source, metadata"
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
                        owner_reference_images: list[dict[str, Any]] = []
                        owner_reference_synced = False
                        try:
                            owner_reference_profile = await asyncio.to_thread(
                                build_owner_tagging_reference_profile,
                                db,
                                person_id_str,
                                show_id=request.show_id,
                                show_name=request.show_name,
                            )
                            raw_refs = owner_reference_profile.get("used")
                            if isinstance(raw_refs, list):
                                owner_reference_images = [entry for entry in raw_refs if isinstance(entry, dict)]
                            if owner_reference_images:
                                owner_reference_images = await asyncio.to_thread(
                                    sync_owner_tagging_reference_usage,
                                    db,
                                    person_id_str,
                                    used_references=owner_reference_images,
                                )
                        except Exception as exc:  # noqa: BLE001
                            logger.warning(
                                "Failed to build owner tagging references for stream person_id=%s error=%s",
                                person_id_str,
                                exc,
                            )
                            owner_reference_images = []

                        show_lookup_by_alias: dict[str, dict[str, Any]] | None = None
                        try:
                            _, show_lookup_by_alias, _ = _build_show_lookup_maps(db)
                        except Exception:  # noqa: BLE001
                            show_lookup_by_alias = {}
                        show_exists_cache: dict[str, bool] = {}
                        show_name_cache: dict[str, str | None] = {}
                        person_name_id_cache: dict[str, str | None] = {}
                        person_id_name_cache: dict[str, str | None] = {}
                        reference_pool_cache: dict[str, list[dict[str, Any]]] = {}
                        to_process: list[dict[str, Any]] = []
                        skipped_existing_rows = 0
                        for row in cast_rows:
                            tag_row = tag_rows.get(str(row["id"]))
                            if has_manual_tags(tag_row):
                                continue
                            metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
                            has_existing_count = bool(tag_row and tag_row.get("people_count") is not None)
                            if has_existing_count and not _has_face_metadata_backfill_needed(metadata):
                                skipped_existing_rows += 1
                                continue
                            urls = _pick_autocount_urls(row)
                            if not urls:
                                continue
                            trr_show_eligible = _is_trr_show_eligible(
                                db,
                                metadata=metadata,
                                request_show_id=request.show_id,
                                request_show_name=request.show_name,
                                show_lookup_by_alias=show_lookup_by_alias,
                                show_exists_cache=show_exists_cache,
                                show_name_cache=show_name_cache,
                            )
                            to_process.append(
                                {
                                    "origin": "cast_photos",
                                    "id": str(row["id"]),
                                    "urls": urls,
                                    "tag_row": tag_row,
                                    "row": row,
                                    "trr_show_eligible": trr_show_eligible,
                                }
                            )

                        for row in media_rows:
                            context = row.get("context") if isinstance(row.get("context"), dict) else {}
                            if has_manual_people_tags(context):
                                continue
                            if has_people_count(context) and not _has_face_metadata_backfill_needed(context):
                                skipped_existing_rows += 1
                                continue
                            urls = _build_media_link_autocount_urls(row)
                            if not urls:
                                continue
                            trr_show_eligible = _is_trr_show_eligible(
                                db,
                                metadata=context,
                                request_show_id=request.show_id,
                                request_show_name=request.show_name,
                                show_lookup_by_alias=show_lookup_by_alias,
                                show_exists_cache=show_exists_cache,
                                show_name_cache=show_name_cache,
                            )
                            to_process.append(
                                {
                                    "origin": "media_links",
                                    "id": str(row["id"]),
                                    "media_asset_id": str(row.get("media_asset_id") or ""),
                                    "urls": urls,
                                    "context": dict(context or {}),
                                    "row": row,
                                    "trr_show_eligible": trr_show_eligible,
                                }
                            )

                        total_to_count = len(to_process)
                        _record_stage_row_stats(
                            auto_count_stage_stats,
                            attempted_rows=total_to_count,
                            skipped_existing_rows=skipped_existing_rows,
                        )
                        if total_to_count == 0:
                            yield progress(
                                {
                                    "stage": "auto_count",
                                    "message": "People count + face crops already up to date (no pending images).",
                                    "current": 0,
                                    "total": 0,
                                    "reviewed_rows": 0,
                                    "changed_rows": 0,
                                    "total_rows": 0,
                                    "failed_rows": 0,
                                    "skip_reason": "no_pending_images",
                                    "attempted_rows": int(auto_count_stage_stats.get("attempted_rows", 0)),
                                    "skipped_existing_rows": int(
                                        auto_count_stage_stats.get("skipped_existing_rows", 0)
                                    ),
                                }
                            )
                        else:
                            yield progress(
                                {
                                    "stage": "auto_count",
                                    "message": "Auto-counting people in images...",
                                    "current": 0,
                                    "total": total_to_count,
                                    "reviewed_rows": 0,
                                    "changed_rows": 0,
                                    "total_rows": total_to_count,
                                    "failed_rows": 0,
                                    "attempted_rows": int(auto_count_stage_stats.get("attempted_rows", 0)),
                                    "skipped_existing_rows": int(
                                        auto_count_stage_stats.get("skipped_existing_rows", 0)
                                    ),
                                    "auto_faces_detected": 0,
                                    "auto_face_crops_generated": 0,
                                    "auto_person_fallback_crops_generated": 0,
                                    "auto_no_face_rows": 0,
                                    "auto_identity_skipped_non_trr_show": 0,
                                }
                            )
                        service_unavailable_error: ScreenalyticsUnavailableError | None = None
                        for idx, entry in enumerate(to_process, start=1):
                            if await _client_disconnected("auto_count"):
                                return
                            allow_identity_assignment = bool(entry.get("trr_show_eligible"))
                            tagged_people_ids = None
                            if entry["origin"] == "cast_photos":
                                tag_row = entry.get("tag_row")
                                tagged_people_ids = tag_row.get("people_ids") if tag_row else None
                                tagged_people_names = tag_row.get("people_names") if tag_row else None
                                row_for_candidates = entry.get("row") if isinstance(entry.get("row"), dict) else {}
                                metadata_signals = [
                                    tagged_people_names,
                                    row_for_candidates.get("people_names"),
                                    row_for_candidates.get("title_names"),
                                    row_for_candidates.get("caption"),
                                    row_for_candidates.get("source_page_url"),
                                    row_for_candidates.get("metadata"),
                                ]
                            else:
                                context_for_candidates = (
                                    entry.get("context") if isinstance(entry.get("context"), dict) else {}
                                )
                                tagged_people_ids = context_for_candidates.get("people_ids")
                                tagged_people_names = context_for_candidates.get("people_names")
                                row_for_candidates = entry.get("row") if isinstance(entry.get("row"), dict) else {}
                                metadata_signals = [
                                    tagged_people_names,
                                    context_for_candidates.get("titles"),
                                    context_for_candidates.get("caption"),
                                    context_for_candidates.get("name"),
                                    context_for_candidates.get("title"),
                                    context_for_candidates.get("episode"),
                                    context_for_candidates.get("original_source_page"),
                                    context_for_candidates,
                                    row_for_candidates.get("caption"),
                                    row_for_candidates.get("metadata"),
                                ]
                            candidate_person_ids = _build_identity_candidate_person_ids(
                                db=db,
                                allow_identity_assignment=allow_identity_assignment,
                                owner_person_id=person_id_str,
                                tagged_people_ids=tagged_people_ids,
                                tagged_people_names=tagged_people_names,
                                metadata_signals=metadata_signals,
                                person_name_id_cache=person_name_id_cache,
                            )
                            person_reference_images = _resolve_runtime_person_reference_pools(
                                db,
                                candidate_person_ids=candidate_person_ids,
                                request_show_id=request.show_id,
                                request_show_name=request.show_name,
                                reference_cache=reference_pool_cache,
                                person_id_name_cache=person_id_name_cache,
                            )
                            auto_counts_attempted += 1
                            result = None
                            selected_image_url: str | None = None
                            last_error: ScreenalyticsClientError | None = None
                            for url in entry["urls"]:
                                try:
                                    if candidate_person_ids:
                                        result = await asyncio.to_thread(
                                            count_people,
                                            url,
                                            candidate_person_ids=candidate_person_ids,
                                            owner_person_id=person_id_str,
                                            owner_reference_images=owner_reference_images or None,
                                            person_reference_images=person_reference_images or None,
                                        )
                                    else:
                                        result = await asyncio.to_thread(
                                            count_people,
                                            url,
                                            owner_person_id=person_id_str,
                                            owner_reference_images=owner_reference_images or None,
                                            person_reference_images=person_reference_images or None,
                                        )
                                    selected_image_url = url
                                    break
                                except TypeError:
                                    if candidate_person_ids:
                                        result = await asyncio.to_thread(
                                            count_people,
                                            url,
                                            candidate_person_ids=candidate_person_ids,
                                        )
                                    else:
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
                                detail = str(service_unavailable_error) or "Vision unavailable"
                                errors.append(f"Auto-count service unavailable: {detail}")
                                yield progress(
                                    {
                                        "stage": "auto_count",
                                        "message": "Auto-count paused (vision unavailable).",
                                        "current": max(0, idx - 1),
                                        "total": total_to_count,
                                        "reviewed_rows": max(0, idx - 1),
                                        "changed_rows": auto_counts_succeeded,
                                        "total_rows": total_to_count,
                                        "failed_rows": auto_counts_failed,
                                        "skip_reason": "service_unavailable",
                                        "service_unavailable": True,
                                        "retry_after_s": retry_after,
                                        "detail": detail,
                                    }
                                )
                                break
                            try:
                                if result is None:
                                    auto_count_diagnostics["auto_detect_failed_rows"] += 1
                                    raise last_error or ScreenalyticsClientError("Unable to auto-count image")
                                auto_count_diagnostics["auto_detect_success_rows"] += 1
                                if not owner_reference_synced and isinstance(
                                    getattr(result, "reference_profile", None), dict
                                ):
                                    used_refs = result.reference_profile.get("used")
                                    if isinstance(used_refs, list):
                                        owner_reference_images = await asyncio.to_thread(
                                            sync_owner_tagging_reference_usage,
                                            db,
                                            person_id_str,
                                            used_references=[entry for entry in used_refs if isinstance(entry, dict)],
                                        )
                                        owner_reference_synced = True
                                if entry["origin"] == "cast_photos":
                                    tag_row = entry.get("tag_row")
                                    existing_people_names = tag_row.get("people_names") if tag_row else None
                                    existing_people_ids = tag_row.get("people_ids") if tag_row else None
                                    if not allow_identity_assignment:
                                        auto_count_diagnostics["auto_identity_skipped_non_trr_show"] += 1
                                    face_boxes, row_diagnostics = _build_detection_boxes(
                                        result,
                                        tagged_people_ids=existing_people_ids,
                                        tagged_people_names=existing_people_names,
                                        owner_person_id=person_id_str,
                                        owner_person_name=person_name,
                                        allow_identity_assignment=allow_identity_assignment,
                                    )
                                    auto_people_ids, auto_people_names = _auto_people_from_face_boxes(face_boxes)
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
                                            existing_people_ids if existing_people_ids else (auto_people_ids or None)
                                        ),
                                        people_count=result.people_count,
                                        people_count_source="auto",
                                        detector=result.detector,
                                        updated_by_firebase_uid="system:auto",
                                    )
                                    _merge_counter_fields(
                                        auto_count_diagnostics,
                                        row_diagnostics,
                                        AUTO_COUNT_DIAGNOSTIC_FIELDS,
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
                                        if face_crops:
                                            auto_count_diagnostics["auto_crop_cache_success_rows"] += 1
                                        else:
                                            auto_count_diagnostics["auto_crop_cache_failed_rows"] += 1
                                    else:
                                        auto_count_diagnostics["auto_crop_cache_success_rows"] += 1
                                    face_crop_counts = _count_face_crop_sources(face_boxes, face_crops)
                                    auto_count_diagnostics["auto_face_crops_generated"] += face_crop_counts[0]
                                    auto_count_diagnostics["auto_person_fallback_crops_generated"] += face_crop_counts[
                                        1
                                    ]
                                    if metadata.get("face_boxes") != face_boxes:
                                        metadata["face_boxes"] = face_boxes
                                        metadata_changed = True
                                    if metadata.get("face_crops") != face_crops:
                                        metadata["face_crops"] = face_crops
                                        metadata_changed = True
                                    crop_payload = _owner_face_crop_payload(
                                        face_boxes,
                                        owner_person_id=person_id_str,
                                        owner_person_name=person_name,
                                    )
                                    if crop_payload is not None:
                                        if not _is_manual_thumbnail_crop(metadata.get("thumbnail_crop")):
                                            metadata["thumbnail_crop"] = crop_payload
                                            metadata_changed = True
                                    if metadata_changed:
                                        await asyncio.to_thread(
                                            lambda metadata_payload=metadata, entry_id=entry["id"]: (
                                                db.schema("core")
                                                .table("cast_photos")
                                                .update({"metadata": metadata_payload})
                                                .eq("id", entry_id)
                                                .execute()
                                            )
                                        )
                                    auto_count_diagnostics["auto_persist_success_rows"] += 1
                                else:
                                    context = dict(entry.get("context") or {})
                                    if not allow_identity_assignment:
                                        auto_count_diagnostics["auto_identity_skipped_non_trr_show"] += 1
                                    face_boxes, row_diagnostics = _build_detection_boxes(
                                        result,
                                        tagged_people_ids=context.get("people_ids"),
                                        tagged_people_names=context.get("people_names"),
                                        owner_person_id=person_id_str,
                                        owner_person_name=person_name,
                                        allow_identity_assignment=allow_identity_assignment,
                                    )
                                    _merge_counter_fields(
                                        auto_count_diagnostics,
                                        row_diagnostics,
                                        AUTO_COUNT_DIAGNOSTIC_FIELDS,
                                    )
                                    auto_people_ids, auto_people_names = _auto_people_from_face_boxes(face_boxes)
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
                                        if face_crops:
                                            auto_count_diagnostics["auto_crop_cache_success_rows"] += 1
                                        else:
                                            auto_count_diagnostics["auto_crop_cache_failed_rows"] += 1
                                    else:
                                        auto_count_diagnostics["auto_crop_cache_success_rows"] += 1
                                    face_crop_counts = _count_face_crop_sources(face_boxes, face_crops)
                                    auto_count_diagnostics["auto_face_crops_generated"] += face_crop_counts[0]
                                    auto_count_diagnostics["auto_person_fallback_crops_generated"] += face_crop_counts[
                                        1
                                    ]
                                    context["face_crops"] = face_crops
                                    if (
                                        not (isinstance(context.get("people_ids"), list) and context.get("people_ids"))
                                        and auto_people_ids
                                    ):
                                        context["people_ids"] = auto_people_ids
                                    if (
                                        not (
                                            isinstance(context.get("people_names"), list)
                                            and context.get("people_names")
                                        )
                                        and auto_people_names
                                    ):
                                        context["people_names"] = auto_people_names
                                    crop_payload = _owner_face_crop_payload(
                                        face_boxes,
                                        owner_person_id=person_id_str,
                                        owner_person_name=person_name,
                                    )
                                    if crop_payload is not None and not _is_manual_thumbnail_crop(
                                        context.get("thumbnail_crop")
                                    ):
                                        context["thumbnail_crop"] = crop_payload
                                    await asyncio.to_thread(
                                        lambda context_payload=context, entry_id=entry["id"]: (
                                            db.schema("core")
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
                                    )
                                    auto_count_diagnostics["auto_persist_success_rows"] += 1
                                auto_counts_succeeded += 1
                            except Exception as exc:  # noqa: BLE001
                                auto_counts_failed += 1
                                if result is not None:
                                    auto_count_diagnostics["auto_persist_failed_rows"] += 1
                                errors.append(f"Auto-count {entry['id']}: {exc}")
                                continue

                            if idx <= 20 or idx % 5 == 0 or idx == total_to_count:
                                yield progress(
                                    {
                                        "stage": "auto_count",
                                        "message": "Auto-counting people in images...",
                                        "current": idx,
                                        "total": total_to_count,
                                        "reviewed_rows": idx,
                                        "changed_rows": auto_counts_succeeded,
                                        "total_rows": total_to_count,
                                        "failed_rows": auto_counts_failed,
                                        "attempted_rows": int(auto_count_stage_stats.get("attempted_rows", 0)),
                                        "skipped_existing_rows": int(
                                            auto_count_stage_stats.get("skipped_existing_rows", 0)
                                        ),
                                        "auto_faces_detected": int(
                                            auto_count_diagnostics.get("auto_faces_detected", 0)
                                        ),
                                        "auto_face_crops_generated": int(
                                            auto_count_diagnostics.get("auto_face_crops_generated", 0)
                                        ),
                                        "auto_person_fallback_crops_generated": int(
                                            auto_count_diagnostics.get("auto_person_fallback_crops_generated", 0)
                                        ),
                                        "auto_no_face_rows": int(auto_count_diagnostics.get("auto_no_face_rows", 0)),
                                        "auto_identity_skipped_non_trr_show": int(
                                            auto_count_diagnostics.get("auto_identity_skipped_non_trr_show", 0)
                                        ),
                                        "row_error_counts": _build_auto_count_row_error_counts(auto_count_diagnostics),
                                    }
                                )
                else:
                    yield progress(
                        {
                            "stage": "auto_count",
                            "message": "Skipping auto-count (not configured).",
                            "current": 0,
                            "total": 0,
                            "reviewed_rows": 0,
                            "changed_rows": 0,
                            "total_rows": 0,
                            "failed_rows": 0,
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
                    "reviewed_rows": 0,
                    "changed_rows": 0,
                    "total_rows": 0,
                    "failed_rows": 0,
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
                    skipped_existing_rows = 0
                    for row in cast_rows:
                        meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
                        if "has_text_overlay" in (meta or {}):
                            skipped_existing_rows += 1
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
                        context = row.get("context") if isinstance(row.get("context"), dict) else {}
                        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
                        if "has_text_overlay" in context or "has_text_overlay" in metadata:
                            skipped_existing_rows += 1
                            continue
                        to_process.append({"origin": "media_links", "id": asset_id})

                    total_text = len(to_process)
                    text_overlay_candidates = total_text
                    _record_stage_row_stats(
                        text_overlay_stage_stats,
                        attempted_rows=total_text,
                        skipped_existing_rows=skipped_existing_rows,
                    )
                    if total_text == 0:
                        text_overlay_skipped_reason = "no_pending_images"
                        yield progress(
                            {
                                "stage": "word_id",
                                "message": "Text overlay already up to date (no pending images).",
                                "current": 0,
                                "total": 0,
                                "reviewed_rows": 0,
                                "changed_rows": 0,
                                "total_rows": 0,
                                "failed_rows": 0,
                                "attempted_rows": int(text_overlay_stage_stats.get("attempted_rows", 0)),
                                "skipped_existing_rows": int(text_overlay_stage_stats.get("skipped_existing_rows", 0)),
                            }
                        )
                    else:
                        yield progress(
                            {
                                "stage": "word_id",
                                "message": "Detecting words/text overlays...",
                                "current": 0,
                                "total": total_text,
                                "reviewed_rows": 0,
                                "changed_rows": 0,
                                "total_rows": total_text,
                                "failed_rows": 0,
                                "attempted_rows": int(text_overlay_stage_stats.get("attempted_rows", 0)),
                                "skipped_existing_rows": int(text_overlay_stage_stats.get("skipped_existing_rows", 0)),
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
                                        "reviewed_rows": idx,
                                        "changed_rows": text_overlay_succeeded + text_overlay_unknown,
                                        "total_rows": total_text,
                                        "failed_rows": text_overlay_failed,
                                        "attempted_rows": int(text_overlay_stage_stats.get("attempted_rows", 0)),
                                        "skipped_existing_rows": int(
                                            text_overlay_stage_stats.get("skipped_existing_rows", 0)
                                        ),
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
                            "reviewed_rows": 0,
                            "changed_rows": 0,
                            "total_rows": 0,
                            "failed_rows": 0,
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
                    "reviewed_rows": 0,
                    "changed_rows": 0,
                    "total_rows": 0,
                    "failed_rows": 0,
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
                                "message": "Skipping centering/cropping (vision unavailable).",
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
                                .select("id, hosted_url, url, image_url, thumb_url, source_page_url, source, metadata")
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
                                "reviewed_rows": 0,
                                "changed_rows": 0,
                                "total_rows": total_crop,
                                "failed_rows": 0,
                                "skipped_manual_rows": centering_skipped_manual,
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
                                detail = str(service_unavailable_error) or "Vision unavailable"
                                errors.append(f"Centering service unavailable: {detail}")
                                yield progress(
                                    {
                                        "stage": "centering_cropping",
                                        "message": "Centering/cropping paused (vision unavailable).",
                                        "current": max(0, idx - 1),
                                        "total": total_crop,
                                        "reviewed_rows": max(0, idx - 1),
                                        "changed_rows": centering_succeeded,
                                        "total_rows": total_crop,
                                        "failed_rows": centering_failed,
                                        "skipped_manual_rows": centering_skipped_manual,
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
                                        lambda metadata_payload=metadata, entry_id=entry["id"]: (
                                            db.schema("core")
                                            .table("cast_photos")
                                            .update({"metadata": metadata_payload})
                                            .eq("id", entry_id)
                                            .execute()
                                        )
                                    )
                                else:
                                    context = dict(entry["context"] or {})
                                    context["thumbnail_crop"] = crop_payload
                                    await asyncio.to_thread(
                                        lambda context_payload=context, entry_id=entry["id"]: (
                                            db.schema("core")
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
                                        "reviewed_rows": idx,
                                        "changed_rows": centering_succeeded,
                                        "total_rows": total_crop,
                                        "failed_rows": centering_failed,
                                        "skipped_manual_rows": centering_skipped_manual,
                                    }
                                )
                else:
                    yield progress(
                        {
                            "stage": "centering_cropping",
                            "message": "Skipping centering/cropping (not configured).",
                            "current": 0,
                            "total": 0,
                            "reviewed_rows": 0,
                            "changed_rows": 0,
                            "total_rows": 0,
                            "failed_rows": 0,
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
                    "reviewed_rows": 0,
                    "changed_rows": 0,
                    "total_rows": 0,
                    "failed_rows": 0,
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
                        "reviewed_rows": 0,
                        "changed_rows": 0,
                        "total_rows": 1,
                        "failed_rows": 0,
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
                    if await _client_disconnected("resizing"):
                        resize_task.cancel()
                        return
                    with resize_progress_lock:
                        progress_current = resize_progress_current
                        progress_total = resize_progress_total
                    yield progress(
                        {
                            "stage": "resizing",
                            "message": "Generating resized variants...",
                            "current": progress_current,
                            "total": progress_total,
                            "reviewed_rows": progress_current,
                            "changed_rows": 0,
                            "total_rows": progress_total,
                            "failed_rows": 0,
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
                        "reviewed_rows": min(resize_processed_ops, resize_total_ops),
                        "changed_rows": resize_succeeded + resize_crop_succeeded,
                        "total_rows": resize_total_ops,
                        "failed_rows": resize_failed + resize_crop_failed,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Resizing: {exc}")

        # 6. Complete
        row_error_counts = _build_auto_count_row_error_counts(auto_count_diagnostics)
        failed_parts = _build_failed_parts_summary(
            metadata_enrichment_failed=metadata_enrichment_failed,
            auto_counts_failed=auto_counts_failed,
            row_error_counts=row_error_counts,
            text_overlay_failed=text_overlay_failed,
            text_overlay_failure_reasons=text_overlay_reason_counts,
            centering_failed=centering_failed,
            resize_failed=resize_failed,
            resize_crop_failed=resize_crop_failed,
        )
        complete_data = {
            "run_id": run_id,
            "person_id": person_id_str,
            "photos_fetched": len(photos) + nbcumv_photos_fetched + getty_only_imported,
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
            "nbcumv_photos_fetched": nbcumv_photos_fetched,
            "nbcumv_assets_imported": nbcumv_assets_imported,
            "nbcumv_assets_skipped": nbcumv_assets_skipped,
            "nbcumv_gallery_links_created": nbcumv_gallery_links_created,
            "nbcumv_failed": nbcumv_failed,
            "getty_candidates_total": getty_candidates_total,
            "getty_matched_total": getty_matched_total,
            "getty_unmatched_total": getty_unmatched_total,
            "shared_nbcumv_total": shared_nbcumv_total,
            "shared_nbcumv_imported": shared_nbcumv_imported,
            "nbcumv_only_total": nbcumv_only_total,
            "nbcumv_only_imported": nbcumv_only_imported,
            "getty_only_imported": getty_only_imported,
            "getty_snapshot_saved": getty_snapshot_saved,
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
            "auto_count_attempted_rows": int(auto_count_stage_stats.get("attempted_rows", 0)),
            "auto_count_skipped_existing_rows": int(auto_count_stage_stats.get("skipped_existing_rows", 0)),
            "auto_count_retry_attempted_rows": int(auto_count_stage_stats.get("retry_attempted_rows", 0)),
            "auto_count_retry_succeeded_rows": int(auto_count_stage_stats.get("retry_succeeded_rows", 0)),
            "auto_faces_detected": int(auto_count_diagnostics.get("auto_faces_detected", 0)),
            "auto_face_crops_generated": int(auto_count_diagnostics.get("auto_face_crops_generated", 0)),
            "auto_person_fallback_crops_generated": int(
                auto_count_diagnostics.get("auto_person_fallback_crops_generated", 0)
            ),
            "auto_no_face_rows": int(auto_count_diagnostics.get("auto_no_face_rows", 0)),
            "auto_identity_skipped_non_trr_show": int(
                auto_count_diagnostics.get("auto_identity_skipped_non_trr_show", 0)
            ),
            "auto_detect_success_rows": int(auto_count_diagnostics.get("auto_detect_success_rows", 0)),
            "auto_detect_failed_rows": int(auto_count_diagnostics.get("auto_detect_failed_rows", 0)),
            "auto_persist_success_rows": int(auto_count_diagnostics.get("auto_persist_success_rows", 0)),
            "auto_persist_failed_rows": int(auto_count_diagnostics.get("auto_persist_failed_rows", 0)),
            "auto_crop_cache_success_rows": int(auto_count_diagnostics.get("auto_crop_cache_success_rows", 0)),
            "auto_crop_cache_failed_rows": int(auto_count_diagnostics.get("auto_crop_cache_failed_rows", 0)),
            "row_error_counts": row_error_counts,
            "text_overlay_attempted": text_overlay_attempted,
            "text_overlay_succeeded": text_overlay_succeeded,
            "text_overlay_unknown": text_overlay_unknown,
            "text_overlay_failed": text_overlay_failed,
            "text_overlay_attempted_rows": int(text_overlay_stage_stats.get("attempted_rows", 0)),
            "text_overlay_skipped_existing_rows": int(text_overlay_stage_stats.get("skipped_existing_rows", 0)),
            "text_overlay_retry_attempted_rows": int(text_overlay_stage_stats.get("retry_attempted_rows", 0)),
            "text_overlay_retry_succeeded_rows": int(text_overlay_stage_stats.get("retry_succeeded_rows", 0)),
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
            "retry_attempts": {
                "auto_count": 1,
                "word_id": 1,
                "centering_cropping": 1,
                "resizing": 1,
            },
            "failed_parts": failed_parts,
            "sources_skipped": len(source_skip_details),
            "source_skip_details": source_skip_details,
            "source_progress": source_progress_snapshot(),
            "live_counts": build_live_counts(),
            "errors": errors,
        }
        yield f"event: complete\ndata: {json.dumps(envelope(complete_data))}\n\n"

    async def guarded_event_generator() -> AsyncGenerator[str, None]:
        try:
            async for chunk in event_generator():
                yield chunk
        except Exception as exc:  # noqa: BLE001
            logger.exception("Refresh stream runtime failure for %s: %s", person_id_str, exc)
            payload = {
                "operation_id": operation_id,
                "event_seq": 0,
                "run_id": run_id,
                "stage": "stream",
                "error": "Refresh stream failed",
                "detail": str(exc),
                "stage_error_code": "STREAM_RUNTIME_FAILED",
                "stage_error_detail": str(exc),
                "checkpoint": "stream_runtime_failed",
                "stream_state": "failed",
                "is_terminal": True,
            }
            yield f"event: error\ndata: {json.dumps(payload)}\n\n"

    stream_response = StreamingResponse(
        guarded_event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
    if _is_internal_raw_stream_request(connection):
        return stream_response

    actor = str((admin_user or {}).get("email") or (admin_user or {}).get("id") or "admin")
    request_payload = {
        "person_id": person_id_str,
        "payload": request.model_dump(mode="json"),
        "request_id": request_id,
        "initiated_by": actor,
    }
    operation = start_operation_for_stream(
        operation_type="admin_person_refresh_images",
        producer=guarded_event_generator,
        request_payload=request_payload,
        initiated_by=actor,
        request=connection,
    )
    return operation_stream_response(str(operation.get("id")), request=connection)


@router.post("/{person_id}/reprocess-images/stream")
async def reprocess_person_images_stream(
    person_id: UUID,
    connection: Request,
    request: ReprocessImagesRequest = Body(default_factory=ReprocessImagesRequest),
    db: SupabaseAdminClient = None,
    admin_user: AdminUser = None,
) -> StreamingResponse:
    """Re-run metadata repair, counting, text-ID, centering, and resize on existing photos (no sync/mirror)."""
    person_id_str = str(person_id)
    run_id = f"reprocess-{person_id_str}-{int(datetime.now(UTC).timestamp())}"
    operation_id = "operation-pending"
    request_id = str(connection.headers.get("x-trr-request-id") or "").strip() or None

    async def event_generator() -> AsyncGenerator[str, None]:
        event_seq = 0
        errors: list[str] = []
        execution_profile = _resolve_execution_profile(request.execution_profile)
        run_tagging_stage = request.run_tagging if request.run_tagging is not None else request.run_count
        # Reprocess tagging is intentionally always "full fix" for existing rows.
        # Keep request.force_tagging_recount for backward-compatible payload parsing.
        force_tagging_recount = True
        tagging_batch_size = _resolve_stage_batch_size(
            request_overrides=request.batch_size,
            stage="tagging",
            default=_profile_default_batch_size(execution_profile, "tagging"),
        )
        crop_parallelism = _resolve_stage_parallelism(
            request_overrides=request.max_parallelism,
            stage="crop",
            default=_profile_default_parallelism(execution_profile, "crop"),
        )
        prefer_fast_pass = bool(request.prefer_fast_pass) if request.prefer_fast_pass is not None else True
        text_overlay_reason_counts: dict[str, int] = dict.fromkeys(TEXT_OVERLAY_FAILURE_REASONS, 0)
        existing_imdb_rows_repaired = 0
        metadata_enrichment_failed = 0
        auto_counts_attempted = 0
        auto_counts_failed = 0
        auto_counts_succeeded = 0
        auto_count_diagnostics = _empty_auto_count_diagnostics()
        auto_count_stage_stats = _empty_stage_row_stats()
        c_attempted = 0
        text_overlay_succeeded = 0
        text_overlay_stage_stats = _empty_stage_row_stats()
        c_succeeded = 0
        c_failed = 0
        c_skipped = 0
        resize_attempted = 0
        resize_succeeded = 0
        resize_failed = 0
        resize_crop_attempted = 0
        resize_crop_succeeded = 0
        resize_crop_failed = 0
        retry_attempts = {
            "auto_count": 1,
            "word_id": 1,
            "centering_cropping": 1,
            "resizing": 1,
        }

        def build_live_counts() -> dict[str, int]:
            return {
                "synced": 0,
                "mirrored": 0,
                "counted": int(auto_counts_succeeded),
                "cropped": int(c_succeeded),
                "id_text": int(text_overlay_succeeded),
                "resized": int(resize_succeeded),
            }

        def envelope(payload: dict[str, Any]) -> dict[str, Any]:
            nonlocal event_seq
            event_seq += 1
            return {
                "operation_id": operation_id,
                "event_seq": event_seq,
                **payload,
            }

        def progress(payload: dict[str, Any]) -> str:
            return (
                "event: progress\ndata: "
                + json.dumps(
                    envelope(
                        {
                            "run_id": run_id,
                            "live_counts": build_live_counts(),
                            **payload,
                        }
                    )
                )
                + "\n\n"
            )

        def error_event(
            *,
            stage: str,
            error: str,
            detail: str | None = None,
            stage_error_code: str | None = None,
            stage_error_detail: str | None = None,
        ) -> str:
            payload: dict[str, Any] = {"run_id": run_id, "stage": stage, "error": error}
            if detail:
                payload["detail"] = detail
            if stage_error_code:
                payload["stage_error_code"] = stage_error_code
            if stage_error_detail:
                payload["stage_error_detail"] = stage_error_detail
            return f"event: error\ndata: {json.dumps(envelope(payload))}\n\n"

        async def _client_disconnected(stage: str) -> bool:
            try:
                disconnected = await connection.is_disconnected()
            except Exception:  # noqa: BLE001
                disconnected = False
            if disconnected:
                logger.info("Reprocess stream client disconnected person_id=%s stage=%s", person_id_str, stage)
            return disconnected

        # Verify person exists
        if await _client_disconnected("setup"):
            return
        person = await asyncio.to_thread(_get_person_details, db, person_id_str)
        if not person:
            yield error_event(stage="setup", error="Person not found")
            return

        sources: list[SourceType] = list(request.sources or ALL_SOURCES)
        target_cast_photo_ids = _normalize_scope_ids(request.target_cast_photo_ids)
        target_media_link_ids = _normalize_scope_ids(request.target_media_link_ids)
        scope_active = request.target_cast_photo_ids is not None or request.target_media_link_ids is not None
        no_scoped_targets = scope_active and not target_cast_photo_ids and not target_media_link_ids

        imdb_person_id = _extract_imdb_id(person.get("external_ids") or {})

        # ---------- Metadata repair (IMDb) ----------
        if request.run_metadata:
            if await _client_disconnected("metadata_repair"):
                return
            metadata_repair_progress = {
                "reviewed_rows": 0,
                "changed_rows": 0,
                "total_rows": 0,
                "failed_rows": 0,
            }
            metadata_repair_lock = Lock()

            def _update_metadata_repair_progress(
                reviewed_rows: int,
                total_rows: int,
                changed_rows: int,
                failed_rows: int,
            ) -> None:
                with metadata_repair_lock:
                    metadata_repair_progress["reviewed_rows"] = max(0, int(reviewed_rows))
                    metadata_repair_progress["total_rows"] = max(0, int(total_rows))
                    metadata_repair_progress["changed_rows"] = max(0, int(changed_rows))
                    metadata_repair_progress["failed_rows"] = max(0, int(failed_rows))

            yield progress(
                {
                    "stage": "metadata_repair",
                    "message": "Fixing IMDb Details...",
                    "current": 0,
                    "total": 0,
                    "reviewed_rows": 0,
                    "changed_rows": 0,
                    "total_rows": 0,
                    "failed_rows": 0,
                }
            )
            try:
                wwhl_credit_episode_imdb_ids = await asyncio.to_thread(
                    _load_person_wwhl_episode_imdb_ids_from_credits,
                    db,
                    person_id_str,
                )
                imdb_strict_context = await asyncio.to_thread(
                    _resolve_imdb_traitors_strict_context,
                    db,
                    show_id=request.show_id,
                    show_name=request.show_name,
                    target_person_imdb_id=imdb_person_id,
                    target_person_name=person.get("full_name"),
                )
                metadata_repair_started_at = time.perf_counter()
                metadata_repair_task = asyncio.create_task(
                    asyncio.to_thread(
                        _repair_existing_imdb_cast_photos,
                        db,
                        person_id_str,
                        show_id=request.show_id,
                        show_name=request.show_name,
                        strict_context=imdb_strict_context,
                        wwhl_credit_episode_imdb_ids=wwhl_credit_episode_imdb_ids,
                        progress_cb=_update_metadata_repair_progress,
                    )
                )
                while not metadata_repair_task.done():
                    await asyncio.sleep(2)
                    if metadata_repair_task.done():
                        break
                    if await _client_disconnected("metadata_repair"):
                        metadata_repair_task.cancel()
                        return
                    with metadata_repair_lock:
                        metadata_repair_snapshot = dict(metadata_repair_progress)
                    reviewed_rows = int(metadata_repair_snapshot.get("reviewed_rows", 0))
                    total_rows = int(metadata_repair_snapshot.get("total_rows", 0))
                    yield progress(
                        {
                            "stage": "metadata_repair",
                            "message": "Fixing IMDb Details...",
                            "current": reviewed_rows,
                            "total": total_rows,
                            "reviewed_rows": reviewed_rows,
                            "changed_rows": int(metadata_repair_snapshot.get("changed_rows", 0)),
                            "total_rows": total_rows,
                            "failed_rows": int(metadata_repair_snapshot.get("failed_rows", 0)),
                            "heartbeat": True,
                            "elapsed_ms": int((time.perf_counter() - metadata_repair_started_at) * 1000),
                        }
                    )
                existing_imdb_rows_repaired, repair_failed = await metadata_repair_task
                metadata_enrichment_failed += repair_failed
                with metadata_repair_lock:
                    metadata_repair_snapshot = dict(metadata_repair_progress)
                reviewed_rows = int(metadata_repair_snapshot.get("reviewed_rows", 0))
                total_rows = int(metadata_repair_snapshot.get("total_rows", 0))
                changed_rows = int(metadata_repair_snapshot.get("changed_rows", 0))
                failed_rows = int(metadata_repair_snapshot.get("failed_rows", 0))
                yield progress(
                    {
                        "stage": "metadata_repair",
                        "message": (
                            "Fixing IMDb Details complete "
                            f"(reviewed {reviewed_rows}/{total_rows}, "
                            f"changed {changed_rows}, failed {failed_rows})."
                        ),
                        "current": reviewed_rows,
                        "total": total_rows,
                        "reviewed_rows": reviewed_rows,
                        "changed_rows": changed_rows,
                        "total_rows": total_rows,
                        "failed_rows": failed_rows,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                metadata_enrichment_failed += 1
                errors.append(f"Metadata repair: {exc}")
                with metadata_repair_lock:
                    metadata_repair_snapshot = dict(metadata_repair_progress)
                reviewed_rows = int(metadata_repair_snapshot.get("reviewed_rows", 0))
                total_rows = int(metadata_repair_snapshot.get("total_rows", 0))
                yield progress(
                    {
                        "stage": "metadata_repair",
                        "message": f"Fixing IMDb Details failed: {exc}",
                        "current": reviewed_rows,
                        "total": total_rows,
                        "reviewed_rows": reviewed_rows,
                        "changed_rows": int(metadata_repair_snapshot.get("changed_rows", 0)),
                        "total_rows": total_rows,
                        "failed_rows": int(metadata_repair_snapshot.get("failed_rows", 0)),
                    }
                )
        else:
            yield progress(
                {
                    "stage": "metadata_repair",
                    "message": "Skipping metadata repair stage.",
                    "current": 0,
                    "total": 0,
                    "reviewed_rows": 0,
                    "changed_rows": 0,
                    "total_rows": 0,
                    "failed_rows": 0,
                }
            )

        # ---------- Auto-count (cast_photos + media_links) ----------
        if run_tagging_stage and not no_scoped_targets:
            if await _client_disconnected("auto_count"):
                return
            yield progress(
                {
                    "stage": "auto_count",
                    "message": "Auto-counting people in images...",
                    "current": 0,
                    "total": 0,
                    "reviewed_rows": 0,
                    "changed_rows": 0,
                    "total_rows": 0,
                    "failed_rows": 0,
                }
            )

            owner_reference_images: list[dict[str, Any]] = []
            owner_reference_synced = False
            try:
                owner_reference_profile = await asyncio.to_thread(
                    build_owner_tagging_reference_profile,
                    db,
                    person_id_str,
                    show_id=request.show_id,
                    show_name=request.show_name,
                )
                raw_refs = owner_reference_profile.get("used")
                if isinstance(raw_refs, list):
                    owner_reference_images = [entry for entry in raw_refs if isinstance(entry, dict)]
                if owner_reference_images:
                    owner_reference_images = await asyncio.to_thread(
                        sync_owner_tagging_reference_usage,
                        db,
                        person_id_str,
                        used_references=owner_reference_images,
                    )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to build owner tagging references for reprocess stream person_id=%s error=%s",
                    person_id_str,
                    exc,
                )
                owner_reference_images = []

            def _sync_owner_references_once(used_references: list[dict[str, Any]]) -> None:
                nonlocal owner_reference_images, owner_reference_synced
                if owner_reference_synced:
                    return
                try:
                    owner_reference_images = sync_owner_tagging_reference_usage(
                        db,
                        person_id_str,
                        used_references=used_references,
                    )
                    owner_reference_synced = True
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "Failed to sync owner tagging references for reprocess stream person_id=%s error=%s",
                        person_id_str,
                        exc,
                    )

            failed_cast_photo_ids: list[str] = []
            failed_media_link_ids: list[str] = []
            auto_count_progress_lock = Lock()
            auto_count_cast_progress = {"current": 0, "total": 0}
            auto_count_media_progress = {"current": 0, "total": 0}

            def _update_auto_count_cast_progress(current: int, total: int) -> None:
                with auto_count_progress_lock:
                    auto_count_cast_progress["current"] = max(0, int(current))
                    auto_count_cast_progress["total"] = max(0, int(total))

            def _update_auto_count_media_progress(current: int, total: int) -> None:
                with auto_count_progress_lock:
                    auto_count_media_progress["current"] = max(0, int(current))
                    auto_count_media_progress["total"] = max(0, int(total))

            auto_count_cast_task = asyncio.create_task(
                asyncio.to_thread(
                    _auto_count_cast_photos,
                    db,
                    person_id_str,
                    sources,
                    owner_person_name=person.get("full_name"),
                    owner_reference_images=owner_reference_images,
                    owner_reference_sync_cb=_sync_owner_references_once,
                    photo_ids=target_cast_photo_ids,
                    force_recount=force_tagging_recount,
                    request_show_id=request.show_id,
                    request_show_name=request.show_name,
                    progress_cb=_update_auto_count_cast_progress,
                    diagnostics=auto_count_diagnostics,
                    stage_stats=auto_count_stage_stats,
                    failed_photo_ids=failed_cast_photo_ids,
                    tagging_batch_size=tagging_batch_size,
                    prefer_fast_pass=prefer_fast_pass,
                )
            )
            while not auto_count_cast_task.done():
                await asyncio.sleep(2)
                if auto_count_cast_task.done():
                    break
                if await _client_disconnected("auto_count"):
                    auto_count_cast_task.cancel()
                    return
                with auto_count_progress_lock:
                    cast_current = int(auto_count_cast_progress.get("current", 0))
                    cast_total = int(auto_count_cast_progress.get("total", 0))
                yield progress(
                    {
                        "stage": "auto_count",
                        "message": "Auto-counting people in images...",
                        "current": cast_current,
                        "total": cast_total,
                        "reviewed_rows": cast_current,
                        "changed_rows": auto_counts_succeeded,
                        "total_rows": cast_total,
                        "failed_rows": auto_counts_failed,
                        "heartbeat": True,
                        "attempted_rows": int(auto_count_stage_stats.get("attempted_rows", 0)),
                        "skipped_existing_rows": int(auto_count_stage_stats.get("skipped_existing_rows", 0)),
                    }
                )
            ac_cast, sc_cast, fc_cast = await auto_count_cast_task
            # Update counters immediately so build_live_counts() emits
            # accurate values during the media sub-task heartbeat loop.
            auto_counts_succeeded = sc_cast
            auto_counts_failed = fc_cast

            auto_count_media_task = asyncio.create_task(
                asyncio.to_thread(
                    _auto_count_media_links,
                    db,
                    person_id_str,
                    owner_person_name=person.get("full_name"),
                    owner_reference_images=owner_reference_images,
                    owner_reference_sync_cb=_sync_owner_references_once,
                    force_recount=force_tagging_recount,
                    media_link_ids=target_media_link_ids,
                    request_show_id=request.show_id,
                    request_show_name=request.show_name,
                    progress_cb=_update_auto_count_media_progress,
                    diagnostics=auto_count_diagnostics,
                    stage_stats=auto_count_stage_stats,
                    failed_link_ids=failed_media_link_ids,
                    tagging_batch_size=tagging_batch_size,
                    prefer_fast_pass=prefer_fast_pass,
                )
            )
            while not auto_count_media_task.done():
                await asyncio.sleep(2)
                if auto_count_media_task.done():
                    break
                if await _client_disconnected("auto_count"):
                    auto_count_media_task.cancel()
                    return
                with auto_count_progress_lock:
                    cast_total = int(auto_count_cast_progress.get("total", 0))
                    media_current = int(auto_count_media_progress.get("current", 0))
                    media_total = int(auto_count_media_progress.get("total", 0))
                reviewed_rows = cast_total + media_current
                total_rows = cast_total + media_total
                yield progress(
                    {
                        "stage": "auto_count",
                        "message": "Auto-counting people in images...",
                        "current": reviewed_rows,
                        "total": total_rows,
                        "reviewed_rows": reviewed_rows,
                        "changed_rows": auto_counts_succeeded + sc_cast,
                        "total_rows": total_rows,
                        "failed_rows": auto_counts_failed + fc_cast,
                        "heartbeat": True,
                        "attempted_rows": int(auto_count_stage_stats.get("attempted_rows", 0)),
                        "skipped_existing_rows": int(auto_count_stage_stats.get("skipped_existing_rows", 0)),
                    }
                )
            ac_media, sc_media, fc_media = await auto_count_media_task
            auto_counts_attempted = ac_cast + ac_media
            auto_counts_succeeded = sc_cast + sc_media
            auto_counts_failed = fc_cast + fc_media

            if auto_counts_failed > 0:
                retry_attempts["auto_count"] = 2
                yield progress(
                    {
                        "stage": "auto_count",
                        "message": f"Retrying failed auto-count rows ({auto_counts_failed} remaining)...",
                        "current": auto_counts_succeeded,
                        "total": auto_counts_attempted,
                        "retrying": True,
                        "attempt": retry_attempts["auto_count"],
                        "max_attempts": 2,
                        "reviewed_rows": auto_counts_attempted,
                        "changed_rows": auto_counts_succeeded,
                        "total_rows": auto_counts_attempted,
                        "failed_rows": auto_counts_failed,
                        "attempted_rows": int(auto_count_stage_stats.get("attempted_rows", 0)),
                        "skipped_existing_rows": int(auto_count_stage_stats.get("skipped_existing_rows", 0)),
                    }
                )
                retry_cast_failed_ids: list[str] = []
                retry_media_failed_ids: list[str] = []
                ac_cast_retry, sc_cast_retry, fc_cast_retry = await asyncio.to_thread(
                    _auto_count_cast_photos,
                    db,
                    person_id_str,
                    sources,
                    owner_person_name=person.get("full_name"),
                    owner_reference_images=owner_reference_images,
                    owner_reference_sync_cb=_sync_owner_references_once,
                    force_recount=force_tagging_recount,
                    photo_ids=failed_cast_photo_ids or None,
                    request_show_id=request.show_id,
                    request_show_name=request.show_name,
                    diagnostics=auto_count_diagnostics,
                    stage_stats=auto_count_stage_stats,
                    failed_photo_ids=retry_cast_failed_ids,
                    tagging_batch_size=tagging_batch_size,
                    prefer_fast_pass=prefer_fast_pass,
                )
                ac_media_retry, sc_media_retry, fc_media_retry = await asyncio.to_thread(
                    _auto_count_media_links,
                    db,
                    person_id_str,
                    owner_person_name=person.get("full_name"),
                    owner_reference_images=owner_reference_images,
                    owner_reference_sync_cb=_sync_owner_references_once,
                    force_recount=force_tagging_recount,
                    media_link_ids=failed_media_link_ids or None,
                    request_show_id=request.show_id,
                    request_show_name=request.show_name,
                    diagnostics=auto_count_diagnostics,
                    stage_stats=auto_count_stage_stats,
                    failed_link_ids=retry_media_failed_ids,
                    tagging_batch_size=tagging_batch_size,
                    prefer_fast_pass=prefer_fast_pass,
                )
                _record_stage_row_stats(
                    auto_count_stage_stats,
                    retry_attempted_rows=ac_cast_retry + ac_media_retry,
                    retry_succeeded_rows=sc_cast_retry + sc_media_retry,
                )
                auto_counts_attempted += ac_cast_retry + ac_media_retry
                auto_counts_succeeded += sc_cast_retry + sc_media_retry
                auto_counts_failed = fc_cast_retry + fc_media_retry

            if auto_counts_attempted == 0 and auto_counts_failed == 0:
                yield progress(
                    {
                        "stage": "auto_count",
                        "message": "People count + face crops already up to date (no pending images).",
                        "current": 0,
                        "total": 0,
                        "reviewed_rows": 0,
                        "changed_rows": 0,
                        "total_rows": 0,
                        "failed_rows": 0,
                        "skip_reason": "no_pending_images",
                        "attempted_rows": int(auto_count_stage_stats.get("attempted_rows", 0)),
                        "skipped_existing_rows": int(auto_count_stage_stats.get("skipped_existing_rows", 0)),
                    }
                )
            else:
                yield progress(
                    {
                        "stage": "auto_count",
                        "message": (
                            f"Counted {auto_counts_succeeded} images"
                            + (f" ({auto_counts_failed} failed)" if auto_counts_failed > 0 else "")
                            + "."
                        ),
                        "current": auto_counts_attempted,
                        "total": auto_counts_attempted,
                        "reviewed_rows": auto_counts_attempted,
                        "changed_rows": auto_counts_succeeded,
                        "total_rows": auto_counts_attempted,
                        "failed_rows": auto_counts_failed,
                        "attempted_rows": int(auto_count_stage_stats.get("attempted_rows", 0)),
                        "skipped_existing_rows": int(auto_count_stage_stats.get("skipped_existing_rows", 0)),
                        "retry_attempted_rows": int(auto_count_stage_stats.get("retry_attempted_rows", 0)),
                        "retry_succeeded_rows": int(auto_count_stage_stats.get("retry_succeeded_rows", 0)),
                        "auto_faces_detected": int(auto_count_diagnostics.get("auto_faces_detected", 0)),
                        "auto_face_crops_generated": int(auto_count_diagnostics.get("auto_face_crops_generated", 0)),
                        "auto_person_fallback_crops_generated": int(
                            auto_count_diagnostics.get("auto_person_fallback_crops_generated", 0)
                        ),
                        "auto_no_face_rows": int(auto_count_diagnostics.get("auto_no_face_rows", 0)),
                        "auto_identity_skipped_non_trr_show": int(
                            auto_count_diagnostics.get("auto_identity_skipped_non_trr_show", 0)
                        ),
                        "row_error_counts": _build_auto_count_row_error_counts(auto_count_diagnostics),
                    }
                )
        else:
            yield progress(
                {
                    "stage": "auto_count",
                    "message": (
                        "Skipping tagging stage (no scoped targets)."
                        if no_scoped_targets
                        else "Skipping tagging stage."
                    ),
                    "current": 0,
                    "total": 0,
                    "reviewed_rows": 0,
                    "changed_rows": 0,
                    "total_rows": 0,
                    "failed_rows": 0,
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
        text_overlay_skipped_existing_rows = 0

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
                    "reviewed_rows": 0,
                    "changed_rows": 0,
                    "total_rows": 0,
                    "failed_rows": 0,
                }
            )

        if request.run_id_text and no_scoped_targets:
            text_overlay_skipped_reason = "no_scoped_targets"
            yield progress(
                {
                    "stage": "word_id",
                    "message": "Skipping word detection (no scoped targets).",
                    "current": 0,
                    "total": 0,
                    "reviewed_rows": 0,
                    "changed_rows": 0,
                    "total_rows": 0,
                    "failed_rows": 0,
                }
            )
        elif request.run_id_text and text_overlay_configured:
            if await _client_disconnected("word_id"):
                return
            try:
                allowed_cast_ids = set(target_cast_photo_ids or [])
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
                    row_id = str(row.get("id") or "").strip()
                    if allowed_cast_ids and row_id not in allowed_cast_ids:
                        continue
                    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
                    if "has_text_overlay" in metadata:
                        text_overlay_skipped_existing_rows += 1
                        continue
                    if row_id:
                        cast_candidate_ids.append(row_id)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Word ID candidate cast lookup: {exc}")

            try:
                media_rows = await asyncio.to_thread(
                    _fetch_person_media_link_rows,
                    db,
                    person_id_str,
                    link_ids=target_media_link_ids,
                )
                seen_asset_ids: set[str] = set()
                for row in media_rows:
                    asset_id = str(row.get("media_asset_id") or "")
                    if not asset_id or asset_id in seen_asset_ids:
                        continue
                    seen_asset_ids.add(asset_id)
                    context = row.get("context") if isinstance(row.get("context"), dict) else {}
                    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
                    if "has_text_overlay" in context or "has_text_overlay" in metadata:
                        text_overlay_skipped_existing_rows += 1
                        continue
                    media_candidate_ids.append(asset_id)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Word ID candidate media lookup: {exc}")

            text_overlay_candidates = len(cast_candidate_ids) + len(media_candidate_ids)
            _record_stage_row_stats(
                text_overlay_stage_stats,
                attempted_rows=text_overlay_candidates,
                skipped_existing_rows=text_overlay_skipped_existing_rows,
            )

            if text_overlay_candidates == 0:
                text_overlay_skipped_reason = "no_pending_images"
                yield progress(
                    {
                        "stage": "word_id",
                        "message": "Text overlay already up to date (no pending images).",
                        "current": 0,
                        "total": 0,
                        "reviewed_rows": 0,
                        "changed_rows": 0,
                        "total_rows": 0,
                        "failed_rows": 0,
                        "attempted_rows": int(text_overlay_stage_stats.get("attempted_rows", 0)),
                        "skipped_existing_rows": int(text_overlay_stage_stats.get("skipped_existing_rows", 0)),
                    }
                )
            else:
                yield progress(
                    {
                        "stage": "word_id",
                        "message": "Detecting words/text overlays...",
                        "current": 0,
                        "total": text_overlay_candidates,
                        "reviewed_rows": 0,
                        "changed_rows": 0,
                        "total_rows": text_overlay_candidates,
                        "failed_rows": 0,
                        "attempted_rows": int(text_overlay_stage_stats.get("attempted_rows", 0)),
                        "skipped_existing_rows": int(text_overlay_stage_stats.get("skipped_existing_rows", 0)),
                    }
                )

                failed_cast_retry_ids: list[str] = []
                failed_media_retry_ids: list[str] = []
                text_overlay_progress_lock = Lock()
                cast_progress = {"current": 0, "total": 0}
                media_progress = {"current": 0, "total": 0}

                def _update_cast_progress(current: int, total: int) -> None:
                    with text_overlay_progress_lock:
                        cast_progress["current"] = max(0, int(current))
                        cast_progress["total"] = max(0, int(total))

                def _update_media_progress(current: int, total: int) -> None:
                    with text_overlay_progress_lock:
                        media_progress["current"] = max(0, int(current))
                        media_progress["total"] = max(0, int(total))

                cast_task = asyncio.create_task(
                    asyncio.to_thread(
                        _detect_text_overlay_cast_photos,
                        db,
                        person_id_str,
                        sources,
                        photo_ids=cast_candidate_ids,
                        progress_cb=_update_cast_progress,
                        reason_counts=text_overlay_reason_counts,
                        stage_stats=None,
                        failed_photo_ids=failed_cast_retry_ids,
                    )
                )
                while not cast_task.done():
                    await asyncio.sleep(2)
                    if cast_task.done():
                        break
                    if await _client_disconnected("word_id"):
                        cast_task.cancel()
                        return
                    with text_overlay_progress_lock:
                        cast_current = int(cast_progress.get("current", 0))
                        cast_total = int(cast_progress.get("total", 0))
                    yield progress(
                        {
                            "stage": "word_id",
                            "message": "Detecting words/text overlays...",
                            "current": cast_current,
                            "total": cast_total,
                            "reviewed_rows": cast_current,
                            "changed_rows": text_overlay_succeeded + text_overlay_unknown,
                            "total_rows": cast_total,
                            "failed_rows": text_overlay_failed,
                            "heartbeat": True,
                            "attempted_rows": int(text_overlay_stage_stats.get("attempted_rows", 0)),
                            "skipped_existing_rows": int(text_overlay_stage_stats.get("skipped_existing_rows", 0)),
                        }
                    )
                to_cast, ts_cast, tu_cast, tf_cast = await cast_task
                # Update counters immediately so build_live_counts() emits
                # accurate values during the media sub-task heartbeat loop.
                text_overlay_succeeded = ts_cast
                text_overlay_unknown = tu_cast
                text_overlay_failed = tf_cast

                media_task = asyncio.create_task(
                    asyncio.to_thread(
                        _detect_text_overlay_media_links,
                        db,
                        person_id_str,
                        asset_ids=media_candidate_ids,
                        progress_cb=_update_media_progress,
                        reason_counts=text_overlay_reason_counts,
                        stage_stats=None,
                        failed_asset_ids=failed_media_retry_ids,
                    )
                )
                while not media_task.done():
                    await asyncio.sleep(2)
                    if media_task.done():
                        break
                    if await _client_disconnected("word_id"):
                        media_task.cancel()
                        return
                    with text_overlay_progress_lock:
                        cast_total = int(cast_progress.get("total", 0))
                        media_current = int(media_progress.get("current", 0))
                        media_total = int(media_progress.get("total", 0))
                    reviewed_rows = cast_total + media_current
                    total_rows = cast_total + media_total
                    yield progress(
                        {
                            "stage": "word_id",
                            "message": "Detecting words/text overlays...",
                            "current": reviewed_rows,
                            "total": total_rows,
                            "reviewed_rows": reviewed_rows,
                            "changed_rows": text_overlay_succeeded + text_overlay_unknown + ts_cast + tu_cast,
                            "total_rows": total_rows,
                            "failed_rows": text_overlay_failed + tf_cast,
                            "heartbeat": True,
                            "attempted_rows": int(text_overlay_stage_stats.get("attempted_rows", 0)),
                            "skipped_existing_rows": int(text_overlay_stage_stats.get("skipped_existing_rows", 0)),
                        }
                    )
                to_media, ts_media, tu_media, tf_media = await media_task
                text_overlay_attempted = to_cast + to_media
                text_overlay_succeeded = ts_cast + ts_media
                text_overlay_unknown = tu_cast + tu_media
                text_overlay_failed = tf_cast + tf_media

                if text_overlay_failed > 0:
                    retry_attempts["word_id"] = 2
                    yield progress(
                        {
                            "stage": "word_id",
                            "message": f"Retrying failed text overlay rows ({text_overlay_failed} remaining)...",
                            "current": text_overlay_succeeded + text_overlay_unknown,
                            "total": text_overlay_candidates,
                            "retrying": True,
                            "attempt": retry_attempts["word_id"],
                            "max_attempts": 2,
                            "reviewed_rows": text_overlay_succeeded + text_overlay_unknown,
                            "changed_rows": text_overlay_succeeded + text_overlay_unknown,
                            "total_rows": text_overlay_candidates,
                            "failed_rows": text_overlay_failed,
                            "attempted_rows": int(text_overlay_stage_stats.get("attempted_rows", 0)),
                            "skipped_existing_rows": int(text_overlay_stage_stats.get("skipped_existing_rows", 0)),
                        }
                    )
                    retry_failed_cast_ids: list[str] = []
                    retry_failed_media_ids: list[str] = []
                    with text_overlay_progress_lock:
                        cast_progress["current"] = 0
                        cast_progress["total"] = 0
                        media_progress["current"] = 0
                        media_progress["total"] = 0
                    retry_cast_task = asyncio.create_task(
                        asyncio.to_thread(
                            _detect_text_overlay_cast_photos,
                            db,
                            person_id_str,
                            sources,
                            photo_ids=failed_cast_retry_ids or None,
                            progress_cb=_update_cast_progress,
                            reason_counts=text_overlay_reason_counts,
                            stage_stats=None,
                            failed_photo_ids=retry_failed_cast_ids,
                        )
                    )
                    while not retry_cast_task.done():
                        await asyncio.sleep(2)
                        if retry_cast_task.done():
                            break
                        if await _client_disconnected("word_id"):
                            retry_cast_task.cancel()
                            return
                        with text_overlay_progress_lock:
                            retry_cast_current = int(cast_progress.get("current", 0))
                        yield progress(
                            {
                                "stage": "word_id",
                                "message": "Retrying text overlay detection...",
                                "current": text_overlay_succeeded + text_overlay_unknown + retry_cast_current,
                                "total": text_overlay_candidates,
                                "reviewed_rows": text_overlay_succeeded + text_overlay_unknown + retry_cast_current,
                                "changed_rows": text_overlay_succeeded + text_overlay_unknown,
                                "total_rows": text_overlay_candidates,
                                "failed_rows": text_overlay_failed,
                                "heartbeat": True,
                                "retrying": True,
                                "attempt": retry_attempts["word_id"],
                                "max_attempts": 2,
                            }
                        )
                    to_cast_retry, ts_cast_retry, tu_cast_retry, tf_cast_retry = await retry_cast_task

                    with text_overlay_progress_lock:
                        media_progress["current"] = 0
                        media_progress["total"] = 0
                    retry_media_task = asyncio.create_task(
                        asyncio.to_thread(
                            _detect_text_overlay_media_links,
                            db,
                            person_id_str,
                            asset_ids=failed_media_retry_ids or None,
                            progress_cb=_update_media_progress,
                            reason_counts=text_overlay_reason_counts,
                            stage_stats=None,
                            failed_asset_ids=retry_failed_media_ids,
                        )
                    )
                    while not retry_media_task.done():
                        await asyncio.sleep(2)
                        if retry_media_task.done():
                            break
                        if await _client_disconnected("word_id"):
                            retry_media_task.cancel()
                            return
                        with text_overlay_progress_lock:
                            retry_media_current = int(media_progress.get("current", 0))
                        yield progress(
                            {
                                "stage": "word_id",
                                "message": "Retrying text overlay detection...",
                                "current": text_overlay_succeeded
                                + text_overlay_unknown
                                + to_cast_retry
                                + retry_media_current,
                                "total": text_overlay_candidates,
                                "reviewed_rows": text_overlay_succeeded
                                + text_overlay_unknown
                                + to_cast_retry
                                + retry_media_current,
                                "changed_rows": text_overlay_succeeded
                                + text_overlay_unknown
                                + ts_cast_retry
                                + tu_cast_retry,
                                "total_rows": text_overlay_candidates,
                                "failed_rows": text_overlay_failed,
                                "heartbeat": True,
                                "retrying": True,
                                "attempt": retry_attempts["word_id"],
                                "max_attempts": 2,
                            }
                        )
                    to_media_retry, ts_media_retry, tu_media_retry, tf_media_retry = await retry_media_task
                    _record_stage_row_stats(
                        text_overlay_stage_stats,
                        retry_attempted_rows=to_cast_retry + to_media_retry,
                        retry_succeeded_rows=ts_cast_retry + ts_media_retry,
                    )
                    text_overlay_attempted += to_cast_retry + to_media_retry
                    text_overlay_succeeded += ts_cast_retry + ts_media_retry
                    text_overlay_unknown += tu_cast_retry + tu_media_retry
                    text_overlay_failed = tf_cast_retry + tf_media_retry

                yield progress(
                    {
                        "stage": "word_id",
                        "message": (
                            "Text detection done "
                            f"({text_overlay_succeeded} succeeded, {text_overlay_unknown} unknown"
                            + (f", {text_overlay_failed} failed" if text_overlay_failed > 0 else "")
                            + ")."
                        ),
                        "current": text_overlay_attempted,
                        "total": text_overlay_candidates,
                        "reviewed_rows": text_overlay_attempted,
                        "changed_rows": text_overlay_succeeded + text_overlay_unknown,
                        "total_rows": text_overlay_candidates,
                        "failed_rows": text_overlay_failed,
                        "attempted_rows": int(text_overlay_stage_stats.get("attempted_rows", 0)),
                        "skipped_existing_rows": int(text_overlay_stage_stats.get("skipped_existing_rows", 0)),
                        "retry_attempted_rows": int(text_overlay_stage_stats.get("retry_attempted_rows", 0)),
                        "retry_succeeded_rows": int(text_overlay_stage_stats.get("retry_succeeded_rows", 0)),
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
                    "reviewed_rows": 0,
                    "changed_rows": 0,
                    "total_rows": 0,
                    "failed_rows": 0,
                }
            )

        # ---------- Centering / cropping ----------
        if request.run_crop and not no_scoped_targets:
            if await _client_disconnected("centering_cropping"):
                return
            centering_progress_lock = Lock()
            centering_progress = {"current": 0, "total": 0}

            def _update_centering_progress(current: int, total: int) -> None:
                with centering_progress_lock:
                    centering_progress["current"] = max(0, int(current))
                    centering_progress["total"] = max(0, int(total))

            yield progress(
                {
                    "stage": "centering_cropping",
                    "message": "Centering/cropping thumbnails...",
                    "current": 0,
                    "total": 0,
                    "reviewed_rows": 0,
                    "changed_rows": 0,
                    "total_rows": 0,
                    "failed_rows": 0,
                    "skipped_manual_rows": 0,
                }
            )

            centering_task = asyncio.create_task(
                asyncio.to_thread(
                    _recenter_person_gallery_images,
                    db,
                    person_id_str,
                    sources,
                    photo_ids=target_cast_photo_ids,
                    media_link_ids=target_media_link_ids,
                    progress_cb=_update_centering_progress,
                    force=True,
                    max_parallelism=crop_parallelism,
                    owner_person_name=person.get("full_name"),
                    prefer_fast_pass=prefer_fast_pass,
                )
            )
            while not centering_task.done():
                await asyncio.sleep(2)
                if centering_task.done():
                    break
                if await _client_disconnected("centering_cropping"):
                    centering_task.cancel()
                    return
                with centering_progress_lock:
                    centering_current = int(centering_progress.get("current", 0))
                    centering_total = int(centering_progress.get("total", 0))
                yield progress(
                    {
                        "stage": "centering_cropping",
                        "message": "Centering/cropping thumbnails...",
                        "current": centering_current,
                        "total": centering_total,
                        "reviewed_rows": centering_current,
                        "changed_rows": c_succeeded,
                        "total_rows": centering_total,
                        "failed_rows": c_failed,
                        "skipped_manual_rows": c_skipped,
                        "heartbeat": True,
                    }
                )

            c_attempted, c_succeeded, c_failed, c_skipped = await centering_task
            if c_failed > 0:
                retry_attempts["centering_cropping"] = 2
                yield progress(
                    {
                        "stage": "centering_cropping",
                        "message": f"Retrying failed centering rows ({c_failed} remaining)...",
                        "current": c_succeeded,
                        "total": c_attempted,
                        "retrying": True,
                        "attempt": retry_attempts["centering_cropping"],
                        "max_attempts": 2,
                        "reviewed_rows": c_attempted,
                        "changed_rows": c_succeeded,
                        "total_rows": c_attempted,
                        "failed_rows": c_failed,
                        "skipped_manual_rows": c_skipped,
                    }
                )
                c_attempted_retry, c_succeeded_retry, c_failed_retry, c_skipped_retry = await asyncio.to_thread(
                    _recenter_person_gallery_images,
                    db,
                    person_id_str,
                    sources,
                    photo_ids=target_cast_photo_ids,
                    media_link_ids=target_media_link_ids,
                    progress_cb=_update_centering_progress,
                    force=False,
                    max_parallelism=crop_parallelism,
                    owner_person_name=person.get("full_name"),
                    prefer_fast_pass=prefer_fast_pass,
                )
                c_attempted += c_attempted_retry
                c_succeeded += c_succeeded_retry
                c_skipped += c_skipped_retry
                c_failed = c_failed_retry

            yield progress(
                {
                    "stage": "centering_cropping",
                    "message": (
                        f"Centered {c_succeeded} thumbnails"
                        + (
                            f" ({c_failed} failed, {c_skipped} manual skipped)"
                            if c_failed > 0
                            else (f" ({c_skipped} manual skipped)" if c_skipped > 0 else "")
                        )
                        + "."
                    ),
                    "current": c_attempted,
                    "total": c_attempted,
                    "reviewed_rows": c_attempted,
                    "changed_rows": c_succeeded,
                    "total_rows": c_attempted,
                    "failed_rows": c_failed,
                    "skipped_manual_rows": c_skipped,
                }
            )
        else:
            yield progress(
                {
                    "stage": "centering_cropping",
                    "message": (
                        "Skipping centering/cropping stage (no scoped targets)."
                        if no_scoped_targets
                        else "Skipping centering/cropping stage."
                    ),
                    "current": 0,
                    "total": 0,
                    "reviewed_rows": 0,
                    "changed_rows": 0,
                    "total_rows": 0,
                    "failed_rows": 0,
                    "skipped_manual_rows": 0,
                }
            )

        # ---------- Resize / variants ----------
        if request.run_resize and not no_scoped_targets:
            if await _client_disconnected("resizing"):
                return
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
                    "reviewed_rows": 0,
                    "changed_rows": 0,
                    "total_rows": 1,
                    "failed_rows": 0,
                }
            )
            resize_task = asyncio.create_task(
                asyncio.to_thread(
                    _resize_person_gallery_images,
                    db,
                    person_id_str,
                    sources,
                    photo_ids=target_cast_photo_ids,
                    media_link_ids=target_media_link_ids,
                    force=True,
                    progress_cb=_update_resize_progress,
                )
            )
            while not resize_task.done():
                await asyncio.sleep(2)
                if resize_task.done():
                    break
                if await _client_disconnected("resizing"):
                    resize_task.cancel()
                    return
                with resize_progress_lock:
                    progress_current = resize_progress_current
                    progress_total = resize_progress_total
                yield progress(
                    {
                        "stage": "resizing",
                        "message": "Generating resized variants...",
                        "current": progress_current,
                        "total": progress_total,
                        "reviewed_rows": progress_current,
                        "changed_rows": 0,
                        "total_rows": progress_total,
                        "failed_rows": 0,
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
            if resize_failed > 0 or resize_crop_failed > 0:
                retry_attempts["resizing"] = 2
                yield progress(
                    {
                        "stage": "resizing",
                        "message": (
                            f"Retrying failed resize operations ({resize_failed + resize_crop_failed} remaining)..."
                        ),
                        "current": resize_succeeded + resize_crop_succeeded,
                        "total": resize_attempted + resize_crop_attempted,
                        "retrying": True,
                        "attempt": retry_attempts["resizing"],
                        "max_attempts": 2,
                        "reviewed_rows": resize_succeeded + resize_crop_succeeded,
                        "changed_rows": resize_succeeded + resize_crop_succeeded,
                        "total_rows": resize_attempted + resize_crop_attempted,
                        "failed_rows": resize_failed + resize_crop_failed,
                    }
                )
                (
                    resize_attempted_retry,
                    resize_succeeded_retry,
                    resize_failed_retry,
                    resize_crop_attempted_retry,
                    resize_crop_succeeded_retry,
                    resize_crop_failed_retry,
                ) = await asyncio.to_thread(
                    _resize_person_gallery_images,
                    db,
                    person_id_str,
                    sources,
                    photo_ids=target_cast_photo_ids,
                    media_link_ids=target_media_link_ids,
                    force=False,
                    progress_cb=_update_resize_progress,
                )
                resize_attempted += resize_attempted_retry
                resize_succeeded += resize_succeeded_retry
                resize_failed = resize_failed_retry
                resize_crop_attempted += resize_crop_attempted_retry
                resize_crop_succeeded += resize_crop_succeeded_retry
                resize_crop_failed = resize_crop_failed_retry
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
                    "reviewed_rows": min(resize_processed_ops, resize_total_ops),
                    "changed_rows": resize_succeeded + resize_crop_succeeded,
                    "total_rows": resize_total_ops,
                    "failed_rows": resize_failed + resize_crop_failed,
                }
            )
        else:
            yield progress(
                {
                    "stage": "resizing",
                    "message": (
                        "Skipping resize stage (no scoped targets)." if no_scoped_targets else "Skipping resize stage."
                    ),
                    "current": 0,
                    "total": 0,
                    "reviewed_rows": 0,
                    "changed_rows": 0,
                    "total_rows": 0,
                    "failed_rows": 0,
                }
            )

        # ---------- Complete ----------
        row_error_counts = _build_auto_count_row_error_counts(auto_count_diagnostics)
        failed_parts = _build_failed_parts_summary(
            metadata_enrichment_failed=metadata_enrichment_failed,
            auto_counts_failed=auto_counts_failed,
            row_error_counts=row_error_counts,
            text_overlay_failed=text_overlay_failed,
            text_overlay_failure_reasons=text_overlay_reason_counts,
            centering_failed=c_failed,
            resize_failed=resize_failed,
            resize_crop_failed=resize_crop_failed,
        )
        complete_data = {
            "person_id": person_id_str,
            "run_id": run_id,
            "metadata_repair_attempted": 1 if request.run_metadata else 0,
            "existing_imdb_rows_repaired": existing_imdb_rows_repaired,
            "metadata_enrichment_failed": metadata_enrichment_failed,
            "auto_counts_attempted": auto_counts_attempted,
            "auto_counts_succeeded": auto_counts_succeeded,
            "auto_counts_failed": auto_counts_failed,
            "auto_count_attempted_rows": int(auto_count_stage_stats.get("attempted_rows", 0)),
            "auto_count_skipped_existing_rows": int(auto_count_stage_stats.get("skipped_existing_rows", 0)),
            "auto_count_retry_attempted_rows": int(auto_count_stage_stats.get("retry_attempted_rows", 0)),
            "auto_count_retry_succeeded_rows": int(auto_count_stage_stats.get("retry_succeeded_rows", 0)),
            "auto_faces_detected": int(auto_count_diagnostics.get("auto_faces_detected", 0)),
            "auto_face_crops_generated": int(auto_count_diagnostics.get("auto_face_crops_generated", 0)),
            "auto_person_fallback_crops_generated": int(
                auto_count_diagnostics.get("auto_person_fallback_crops_generated", 0)
            ),
            "auto_no_face_rows": int(auto_count_diagnostics.get("auto_no_face_rows", 0)),
            "auto_identity_skipped_non_trr_show": int(
                auto_count_diagnostics.get("auto_identity_skipped_non_trr_show", 0)
            ),
            "auto_detect_success_rows": int(auto_count_diagnostics.get("auto_detect_success_rows", 0)),
            "auto_detect_failed_rows": int(auto_count_diagnostics.get("auto_detect_failed_rows", 0)),
            "auto_persist_success_rows": int(auto_count_diagnostics.get("auto_persist_success_rows", 0)),
            "auto_persist_failed_rows": int(auto_count_diagnostics.get("auto_persist_failed_rows", 0)),
            "auto_crop_cache_success_rows": int(auto_count_diagnostics.get("auto_crop_cache_success_rows", 0)),
            "auto_crop_cache_failed_rows": int(auto_count_diagnostics.get("auto_crop_cache_failed_rows", 0)),
            "row_error_counts": row_error_counts,
            "text_overlay_attempted": text_overlay_attempted,
            "text_overlay_succeeded": text_overlay_succeeded,
            "text_overlay_unknown": text_overlay_unknown,
            "text_overlay_failed": text_overlay_failed,
            "text_overlay_attempted_rows": int(text_overlay_stage_stats.get("attempted_rows", 0)),
            "text_overlay_skipped_existing_rows": int(text_overlay_stage_stats.get("skipped_existing_rows", 0)),
            "text_overlay_retry_attempted_rows": int(text_overlay_stage_stats.get("retry_attempted_rows", 0)),
            "text_overlay_retry_succeeded_rows": int(text_overlay_stage_stats.get("retry_succeeded_rows", 0)),
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
            "retry_attempts": retry_attempts,
            "failed_parts": failed_parts,
            "live_counts": build_live_counts(),
            "errors": errors,
        }
        yield f"event: complete\ndata: {json.dumps(envelope(complete_data))}\n\n"

    async def guarded_event_generator() -> AsyncGenerator[str, None]:
        try:
            async for chunk in event_generator():
                yield chunk
        except Exception as exc:  # noqa: BLE001
            logger.exception("Reprocess stream runtime failure for %s: %s", person_id_str, exc)
            payload = {
                "operation_id": operation_id,
                "event_seq": 0,
                "run_id": run_id,
                "stage": "stream",
                "error": "Reprocess stream failed",
                "detail": str(exc),
                "stage_error_code": "STREAM_RUNTIME_FAILED",
                "stage_error_detail": str(exc),
                "checkpoint": "stream_runtime_failed",
                "stream_state": "failed",
                "is_terminal": True,
            }
            yield f"event: error\ndata: {json.dumps(payload)}\n\n"

    stream_response = StreamingResponse(
        guarded_event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
    if _is_internal_raw_stream_request(connection):
        return stream_response

    actor = str((admin_user or {}).get("email") or (admin_user or {}).get("id") or "admin")
    request_payload = {
        "person_id": person_id_str,
        "payload": request.model_dump(mode="json"),
        "request_id": request_id,
        "initiated_by": actor,
    }
    operation = start_operation_for_stream(
        operation_type="admin_person_reprocess_images",
        producer=guarded_event_generator,
        request_payload=request_payload,
        initiated_by=actor,
        request=connection,
    )
    return operation_stream_response(str(operation.get("id")), request=connection)


class _InternalStreamRequest:
    def __init__(self, *, request_id: str | None) -> None:
        self.headers: dict[str, str] = {"x-trr-internal-raw-stream": "1"}
        if request_id:
            self.headers["x-trr-request-id"] = request_id

    async def is_disconnected(self) -> bool:
        return False


def build_person_refresh_images_operation_producer(
    *,
    request_payload: dict[str, Any],
    db: SupabaseAdminClient | None = None,
):
    from trr_backend.db.admin import create_supabase_admin_client

    person_id_str = str(request_payload.get("person_id") or "").strip()
    if not person_id_str:
        raise ValueError("request_payload.person_id is required")

    payload_data = request_payload.get("payload") if isinstance(request_payload.get("payload"), dict) else {}
    payload = RefreshImagesRequest.model_validate(payload_data)
    request_id = str(request_payload.get("request_id") or "").strip() or None
    initiated_by = str(request_payload.get("initiated_by") or "admin")

    def _producer():
        local_db = db or create_supabase_admin_client()
        stream_response = asyncio.run(
            refresh_person_images_stream(
                person_id=UUID(person_id_str),
                connection=_InternalStreamRequest(request_id=request_id),
                request=payload,
                db=local_db,
                admin_user={"id": initiated_by},
            )
        )
        body_iterator = getattr(stream_response, "body_iterator", None)
        if body_iterator is None:
            return []
        return body_iterator

    return _producer


def build_person_reprocess_images_operation_producer(
    *,
    request_payload: dict[str, Any],
    db: SupabaseAdminClient | None = None,
):
    from trr_backend.db.admin import create_supabase_admin_client

    person_id_str = str(request_payload.get("person_id") or "").strip()
    if not person_id_str:
        raise ValueError("request_payload.person_id is required")

    payload_data = request_payload.get("payload") if isinstance(request_payload.get("payload"), dict) else {}
    payload = ReprocessImagesRequest.model_validate(payload_data)
    request_id = str(request_payload.get("request_id") or "").strip() or None
    initiated_by = str(request_payload.get("initiated_by") or "admin")

    def _producer():
        local_db = db or create_supabase_admin_client()
        stream_response = asyncio.run(
            reprocess_person_images_stream(
                person_id=UUID(person_id_str),
                connection=_InternalStreamRequest(request_id=request_id),
                request=payload,
                db=local_db,
                admin_user={"id": initiated_by},
            )
        )
        body_iterator = getattr(stream_response, "body_iterator", None)
        if body_iterator is None:
            return []
        return body_iterator

    return _producer


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
